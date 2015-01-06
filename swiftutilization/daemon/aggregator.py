# -*- coding: utf-8 -*-
from time import time
from datetime import datetime
from random import random
from os.path import join

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
        self.sample_account = '.transfer'
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
        self.container_ring = Ring('/etc/swift', ring_name='container')
        self.sample_rate = 300

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
                    account, timestamp = container.rsplit('_', 1)
                except ValueError:
                    self.logger.debug('ValueError: %s, '
                                      'need more than 1 value to unpack' % \
                                      container)
                else:
                    if float(timestamp) <= (float(time())//self.sample_rate)*self.sample_rate:
                        containers_to_delete.append(container)
                        pool.spawn_n(self.aggregate_container, container)
            pool.waitall()
            for container in containers_to_delete:
                try:
                    self.logger.debug('delete container: %s' % container)
                    self.swift.delete_container(self.sample_account, container,
                                                acceptable_statuses=(2, HTTP_NOT_FOUND, HTTP_CONFLICT))
                except (Exception, Timeout) as err:
                    self.logger.exception(
                        _('Exception while deleting container %s %s') %
                         (container, str(err)))
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

    def aggregate_container(self, container):
        start_time = time()
        try:
            bytes_received = 0
            bytes_sent = 0
            for o in self.swift.iter_objects(self.sample_account, container):
                timestamp, user_id, data, ip, trans_id = \
                   o['name'].split('/', 4)
                transfer = data.split('_', 1)
                bytes_received += int(transfer[0])
                bytes_sent += int(transfer[1])
                self.report_objects += 1
                self.swift.delete_object(self.sample_account, container, o['name'])

            account, timestamp = container.rsplit('_', 1)
            container_count, object_count, bytes_used = \
                self._get_account_info(account)

            date = datetime.utcfromtimestamp(float(timestamp)).isoformat()
            t_object = 'transfer/%s/%d_%d_%d' % (date, bytes_received,
                                                 bytes_sent, self.report_objects)
            u_object = 'usage/%s/%d_%d_%d' % (date, container_count,
                                              object_count, bytes_used)
            self._hidden_update(account, t_object)
            self._hidden_update(account, u_object)
        except (Exception, Timeout) as err:
            self.logger.increment('errors')
            self.logger.exception(
                _('Exception while aggregating sample %s %s') %
                (container, str(err)))

        self.logger.timing_since('timing', start_time)
        self.report()

    def _get_account_info(
            self, account, acceptable_statuses=(2, HTTP_NOT_FOUND)):
        path = self.swift.make_path(account)
        resp = self.swift.make_request('HEAD', path, {}, acceptable_statuses)
        if not resp.status_int // 100 == 2:
            return (0, 0, 0)
        return (int(resp.headers.get('x-account-container-count', 0)),
                int(resp.headers.get('x-account-object-count', 0)),
                int(resp.headers.get('x-account-bytes-used', 0)))

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
