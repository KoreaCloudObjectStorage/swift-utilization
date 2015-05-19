# -*- coding: utf-8 -*-
import hashlib
import urllib
import json
from time import time
from random import random
from os.path import join

from swiftutilization import iso8601_to_timestamp, timestamp_to_iso8601

from eventlet import sleep, Timeout
from eventlet.greenpool import GreenPool
from swift import gettext_ as _
from swift.common.daemon import Daemon
from swift.common.internal_client import InternalClient
from swift.common.utils import get_logger, dump_recon_cache, \
    normalize_timestamp
from swift.common.http import HTTP_NOT_FOUND, HTTP_CONFLICT
from swift.common.bufferedhttp import http_connect
from swift.common.ring import Ring


class UtilizationAggregator(Daemon):
    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='utilization-aggregator')
        self.interval = int(conf.get('interval') or 60)
        self.aggregate_account = '.utilization'
        self.sample_account = '.transfer_record'
        conf_path = conf.get('__file__') or \
                    '/etc/swift/swift-utilization-aggregator.conf'
        request_tries = int(conf.get('request_tries') or 3)
        self.swift = InternalClient(conf_path,
                                    'Swift Utilization Aggregator',
                                    request_tries)
        self.report_interval = int(conf.get('report_interval') or 60)
        self.report_first_time = self.report_last_time = time()
        self.report_containers = 0
        self.report_objects = 0
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = join(self.recon_cache_path, 'object.recon')
        self.concurrency = int(conf.get('concurrency', 1))
        if self.concurrency < 1:
            raise ValueError("concurrency must be set to at least 1")
        self.processes = int(self.conf.get('processes', 0))
        self.process = int(self.conf.get('process', 0))
        self.container_ring = Ring('/etc/swift', ring_name='container')
        self.sample_rate = int(self.conf.get('sample_rate', 600))
        self.last_chk = iso8601_to_timestamp(self.conf.get(
            'service_start'))
        self.kinx_api_url = self.conf.get('kinx_api_url')

    def report(self, final=False):
        if final:
            elapsed = time() - self.report_first_time
            self.logger.info(_('Pass completed in %ds; %d containers,'
                               ' %d objects aggregated') %
                             (elapsed, self.report_containers,
                              self.report_objects))
            dump_recon_cache({'object_aggregation_pass': elapsed,
                              'aggregation_last_pass': self.report_containers},
                             self.rcache, self.logger)

        elif time() - self.report_last_time >= self.report_interval:
            elapsed = time() - self.report_first_time
            self.logger.info(_('Pass so far %ds; %d objects aggregated') %
                             (elapsed, self.report_objects))
            self.report_last_time = time()

    def run_once(self, *args, **kwargs):
        processes, process = self.get_process_values(kwargs)
        pool = GreenPool(self.concurrency)
        self.report_first_time = self.report_last_time = time()
        self.report_objects = 0
        self.report_containers = 0
        containers_to_delete = []
        try:
            self.logger.debug(_('Run begin'))
            containers, objects = \
                self.swift.get_account_info(self.sample_account)
            self.logger.info(_('Pass beginning; %s possible containers; %s '
                               'possible objects') % (containers, objects))
            for c in self.swift.iter_containers(self.sample_account):
                container = c['name']
                try:
                    timestamp, account = container.split('_', 1)
                    timestamp = float(timestamp)
                except ValueError:
                    self.logger.debug('ValueError: %s, '
                                      'need more than 1 value to unpack' % \
                                      container)
                else:
                    if processes > 0:
                        obj_proc = int(hashlib.md5(container).hexdigest(), 16)
                        if obj_proc % processes != process:
                            continue
                    n = (float(time()) // self.sample_rate) * self.sample_rate
                    if timestamp <= n:
                        containers_to_delete.append(container)
                        pool.spawn_n(self.aggregate_container, container)
            pool.waitall()
            for container in containers_to_delete:
                try:
                    self.logger.debug('delete container: %s' % container)
                    self.swift.delete_container(self.sample_account, container,
                                                acceptable_statuses=(
                                                    2, HTTP_NOT_FOUND,
                                                    HTTP_CONFLICT))
                except (Exception, Timeout) as err:
                    self.logger.exception(
                        _('Exception while deleting container %s %s') %
                        (container, str(err)))

            tenants_to_fillup = list()
            for c in self.swift.iter_containers(self.aggregate_account):
                tenant_id = c['name']
                if processes > 0:
                    c_proc = int(hashlib.md5(tenant_id).hexdigest(), 16)
                    if c_proc % processes != process:
                        continue
                    tenants_to_fillup.append(tenant_id)
            # fillup lossed usage data
            self.fillup_lossed_usage_data(tenants_to_fillup)

            self.logger.debug(_('Run end'))
            self.report(final=True)
        except (Exception, Timeout):
            self.logger.exception(_('Unhandled exception'))

    def run_forever(self, *args, **kwargs):
        """
        Executes passes forever, looking for objects to expire.

        :param args: Extra args to fulfill the Daemon interface; this daemon
                     has no additional args.
        :param kwargs: Extra keyword args to fulfill the Daemon interface; this
                       daemon has no additional keyword args.
        """
        sleep(random() * self.interval)
        while True:
            begin = time()
            try:
                self.run_once(*args, **kwargs)
            except (Exception, Timeout):
                self.logger.exception(_('Unhandled exception'))
            elapsed = time() - begin
            if elapsed < self.interval:
                sleep(random() * (self.interval - elapsed))

    def get_process_values(self, kwargs):
        """
        Gets the processes, process from the kwargs if those values exist.

        Otherwise, return processes, process set in the config file.

        :param kwargs: Keyword args passed into the run_forever(), run_once()
                       methods.  They have values specified on the command
                       line when the daemon is run.
        """
        if kwargs.get('processes') is not None:
            processes = int(kwargs['processes'])
        else:
            processes = self.processes

        if kwargs.get('process') is not None:
            process = int(kwargs['process'])
        else:
            process = self.process

        if process < 0:
            raise ValueError(
                'process must be an integer greater than or equal to 0')

        if processes < 0:
            raise ValueError(
                'processes must be an integer greater than or equal to 0')

        if processes and process >= processes:
            raise ValueError(
                'process must be less than or equal to processes')

        return processes, process

    def aggregate_container(self, container):
        start_time = time()
        try:
            objs_to_delete = list()
            bytes_recvs = dict()
            bytes_sents = dict()

            ts, tenant_id, account = container.split('_', 2)
            ts = int(float(ts))

            for o in self.swift.iter_objects(self.sample_account, container):
                name = o['name']
                objs_to_delete.append(name)
                ts, bytes_rv, bytes_st, trans_id, client_ip = name.split('/')
                bill_type = self.get_billtype_by_client_ip(client_ip, ts)
                bytes_recvs[bill_type] = bytes_recvs.get(bill_type,
                                                         0) + int(bytes_rv)
                bytes_sents[bill_type] = bytes_sents.get(bill_type,
                                                         0) + int(bytes_st)
                self.report_objects += 1

            for o in objs_to_delete:
                self.swift.delete_object(self.sample_account, container, o)

            for bill_type, bt_rv in bytes_recvs.items():
                t_object = 'transfer/%d/%d/%d_%d_%d' % (ts, bill_type, bt_rv,
                                                        bytes_sents[bill_type],
                                                        self.report_objects)
                self._hidden_update(tenant_id, t_object)
        except (Exception, Timeout) as err:
            self.logger.increment('errors')
            self.logger.exception(
                _('Exception while aggregating sample %s %s') %
                (container, str(err)))

        self.logger.timing_since('timing', start_time)
        self.report()

    def account_info(self, tenant_id, timestamp):
        path = '/v1/%s/%s?prefix=usage/%d&limit=1' % (self.aggregate_account,
                                                      tenant_id, timestamp)
        resp = self.swift.make_request('GET', path, {}, (2,))
        if len(resp.body) == 0:
            return 0, 0, 0
        usages = resp.body.split('/', 2)[2].rstrip()
        cont_cnt, obj_cnt, bt_used = usages.split('_')
        return int(cont_cnt), int(obj_cnt), int(bt_used)

    def _hidden_update(self, container, obj, method='PUT'):
        hidden_path = '/%s/%s/%s' % (self.aggregate_account, container, obj)
        part, nodes = self.container_ring.get_nodes(self.aggregate_account,
                                                    container)
        for node in nodes:
            ip = node['ip']
            port = node['port']
            dev = node['device']
            action_headers = dict()
            action_headers['user-agent'] = 'aggregator'
            action_headers['X-Timestamp'] = normalize_timestamp(time())
            action_headers['referer'] = 'aggregator-daemon'
            action_headers['x-size'] = '0'
            action_headers['x-content-type'] = "text/plain"
            action_headers['x-etag'] = 'd41d8cd98f00b204e9800998ecf8427e'

            conn = http_connect(ip, port, dev, part, method, hidden_path,
                                action_headers)
            response = conn.getresponse()
            response.read()

    def fillup_lossed_usage_data(self, tenants):
        now = (float(time()) // self.sample_rate) * self.sample_rate
        path = '/v1/%s/%s?prefix=usage/%d&limit=1'

        for t in tenants:
            last = self.last_chk
            cont_cnt = obj_cnt = bt_used = -1
            while last <= now:
                p = path % (self.aggregate_account, t, last)
                resp = self.swift.make_request('GET', p, {}, (2,))
                if len(resp.body) != 0:
                    usages = resp.body.split('/', 2)[2].rstrip()
                    c, o, bt = usages.split('_')
                    cont_cnt = int(c)
                    obj_cnt = int(o)
                    bt_used = int(bt)
                else:
                    before = last - self.sample_rate
                    if cont_cnt == -1:
                        cont_cnt, obj_cnt, bt_used = \
                            self.account_info(self.aggregate_account, before)
                    obj = 'usage/%d/%d_%d_%d' % (last, cont_cnt, obj_cnt,
                                                 bt_used)
                    self._hidden_update(t, obj)
                last += self.sample_rate
        self.last_chk = now

    def get_billtype_by_client_ip(self, client_ip, timestamp):
        end_ts = timestamp_to_iso8601(timestamp + self.sample_rate - 1)
        start_ts = timestamp_to_iso8601(timestamp)

        params = {'start': start_ts, 'end': end_ts}
        path = self.kinx_api_url + '/?%s' % (urllib.urlencode(params))

        data = json.loads(urllib.urlopen(path).read())
        bill_type = -1
        for r in data['ip_ranges']:
            bill_type = r['bill_type']
            for cidr in r['ip_range']:
                if self.ip_in_cidr(client_ip, cidr):
                    return bill_type
        return bill_type

    def ip_in_cidr(self, client_ip, cidr):
        bt_to_bits = lambda b: bin(int(b))[2:].rjust(8, '0')
        ip_to_bits = lambda ip: ''.join([bt_to_bits(b) for b in ip.split('.')])
        client_ip_bits = ip_to_bits(client_ip)
        ip, snet = cidr.split('/')
        ip_bits = ip_to_bits(ip)
        if client_ip_bits[:int(snet)] == ip_bits[:int(snet)]:
            return True
        else:
            return False