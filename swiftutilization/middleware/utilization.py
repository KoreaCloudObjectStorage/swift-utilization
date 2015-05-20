# -*- coding: utf-8 -*-

import json
import time
from datetime import datetime

from swiftutilization import iso8601_to_timestamp, timestamp_to_iso8601

from swift.common.swob import Request, Response
from swift.common.utils import get_logger, InputProxy, \
    normalize_timestamp
from swift.common.wsgi import make_pre_authed_request
from swift.common.ring import Ring
from swift.common.bufferedhttp import http_connect


class UtilizationMiddleware(object):
    def __init__(self, app, conf, *args, **kwargs):
        self.app = app
        self.conf = conf
        self.sample_account = '.transfer_record'
        self.aggregate_account = '.utilization'
        self.logger = get_logger(self.conf, log_route='utilization')
        self.container_ring = Ring('/etc/swift', ring_name='container')
        self.sample_rate = int(self.conf.get('sample_rate', 600))

    def swift_account(self, env, tenant_id):
        path = '/v1/%s/%s?format=json&prefix=account/' \
               % (self.aggregate_account, tenant_id)
        req = make_pre_authed_request(env, 'GET', path)
        req.environ['swift.proxy_access_log_made'] = True
        resp = req.get_response(self.app)
        if resp.status_int == 404:
            return None
        return json.loads(resp.body)[0]['name'].split('/')[1]

    def check_api_call(self, env):
        path = env.get('RAW_PATH_INFO', None)

        if env['REQUEST_METHOD'] == 'GET' and path == '/api/v1/metering':
            return True
        return False

    def get_account_info(self, env, account):
        path = '/v1/%s' % account
        req = make_pre_authed_request(env, 'HEAD', path)
        req.environ['swift.proxy_access_log_made'] = True
        resp = req.get_response(self.app)
        if not  resp.status_int // 100 == 2:
            return (0, 0, 0)
        return (int(resp.headers.get('x-account-container-count', 0)),
                int(resp.headers.get('x-account-object-count', 0)),
                int(resp.headers.get('x-account-bytes-used', 0)))

    def record_usage_data(self, env, tenant_id, account, timestamp):
        path = '/v1/%s/%s?prefix=usage/%d&format=json' % (
            self.aggregate_account, tenant_id, timestamp)
        req = make_pre_authed_request(env, 'GET', path)
        req.environ['swift.proxy_access_log_made'] = True
        resp = req.get_response(self.app)
        if resp.status_int == 404:
            return
        body = json.loads(resp.body)

        if len(body) != 0:
            return

        container_cnt, obj_cnt, bt_used = self.get_account_info(env, account)
        u_object = 'usage/%d/%d_%d_%d' % (timestamp, container_cnt,
                                          obj_cnt, bt_used)

        self.put_hidden_object(self.aggregate_account, tenant_id, u_object)

    def iter_objects(self, env, path, prefix, marker, end, count):
        path_with_params = '%s?format=json&prefix=%s' % (path, prefix)
        seg = ''
        force_break = False
        while count > 0:
            l = 1000 if count > 1000 else count
            count -= 1000
            rpath = path_with_params + ('&marker=%s' % marker) + (
                '&limit=%d' % l)
            req = make_pre_authed_request(env, 'GET', rpath)
            req.environ['swift.proxy_access_log_made'] = True
            resp = req.get_response(self.app)
            segments = json.loads(resp.body)
            for seg in segments:
                name = seg['name']
                record_ts = int(name.split('/')[1])
                if record_ts > end:
                    force_break = True
                    break
                yield name

            if force_break:
                break

            if len(segments) != l:
                break

            if segments:
                marker = seg['name']
            else:
                break

    def retrieve_utilization_data(self, env, tenant_id, start, end, count):
        path = '/v1/%s/%s' % (self.aggregate_account, tenant_id)
        data = dict()
        data['transfer'] = {}
        data['utilization'] = {}
        marker = 'transfer/%d' % start
        data['transfer'] = list()
        data['utilization']['container_count'] = 0
        data['utilization']['object_count'] = 0
        data['utilization']['bytes_used'] = 0

        bytes_recvs = dict()
        bytes_sents = dict()
        req_counts = dict()
        for o in self.iter_objects(env, path, 'transfer/', marker, end, count):
            bill_type = o.split('/')[2]
            bytes_recv, bytes_sent, req_cnt = o.split('/')[3].split('_')
            bytes_recvs[bill_type] = bytes_recvs.get(bill_type, 0) + int(
                bytes_recv)
            bytes_sents[bill_type] = bytes_sents.get(bill_type, 0) + int(
                bytes_sent)
            req_counts[bill_type] = req_counts.get(bill_type, 0) + int(req_cnt)

        for bill_type, bt_rv in bytes_recvs.items():
            d = dict()
            d['bill_type'] = int(bill_type)
            d['bytes_in'] = bt_rv
            d['bytes_out'] = bytes_sents[bill_type]
            d['req_count'] = req_counts[bill_type]
            data['transfer'].append(d)

        last = None
        marker = 'usage/%d' % start
        for o in self.iter_objects(env, path, 'usage/', marker, end, count):
            last = o

        if last:
            container_cnt, obj_cnt, bytes_used = last.split('/')[2].split('_')
            data['utilization']['container_count'] = container_cnt
            data['utilization']['object_count'] = obj_cnt
            data['utilization']['bytes_used'] = bytes_used
        return data

    def GET(self, req):
        start = req.params.get('start')
        tenant_id = req.params.get('tenantid')
        identity = req.environ.get('HTTP_X_IDENTITY_STATUS')
        roles = req.environ.get('keystone.identity', None)

        if identity == 'Invalid' or not roles or 'admin' not in roles['roles']:
            return Response(request=req, status="403 Forbidden",
                            body="Access Denied",
                            content_type="text/plain")

        if not tenant_id:
            return Response(request=req, status="400 Bad Request",
                            body="tenant_id parameter doesn't exist",
                            content_type="text/plain")

        if not start:
            return Response(request=req, status="400 Bad Request",
                            body="start parameter doesn't exist",
                            content_type="text/plain")

        end = req.params.get('end')
        if end is None:
            end = datetime.utcfromtimestamp(int(time.time())).isoformat()

        # check if tenant_id's users utilization was recorded.
        account = self.swift_account(req.environ.copy(), tenant_id)
        if not account:
            return Response(status="400 Bad Request",
                            content_type="text/plain",
                            body="This tenant_id never used.")

        try:
            # start time is "rounded down"
            start_ts = iso8601_to_timestamp(start)
            # end time is "rounded up"
            end_ts = iso8601_to_timestamp(end)
        except ValueError:
            return Response(status="400 Bad Request",
                            content_type="text/plain",
                            body="start or end time is incorrect format."
                                 "please check start or end parameter")
        if start_ts > end_ts:
            return Response(status="400 Bad Request",
                            content_type="text/plain",
                            body="start time must be before the end time")

        end_ts = (end_ts // 3600 + 1) * 3600
        start_ts = (start_ts // 3600) * 3600

        objsize = (end_ts - start_ts) / self.sample_rate

        content = self.retrieve_utilization_data(req.environ.copy(), tenant_id,
                                                 start_ts, end_ts, objsize)

        content['period_start'] = timestamp_to_iso8601(start_ts)
        content['period_end'] = timestamp_to_iso8601(end_ts)
        content['tenant_id'] = tenant_id
        content['swift_account'] = account
        return Response(request=req, body=json.dumps(content),
                        content_type="application/json")

    def __call__(self, env, start_response):
        self.logger.debug('Calling Utilization Middleware')

        req = Request(env)
        if self.check_api_call(env):
            return self.GET(req)(env, start_response)

        try:
            version, account, container, obj = req.split_path(2, 4, True)
        except ValueError:
            return self.app(env, start_response)

        remote_user = env.get('REMOTE_USER')
        if not remote_user or (isinstance(remote_user, basestring) and
                               remote_user.startswith('.wsgi')):
            self.logger.debug('### SKIP: REMOTE_USER is %s' % remote_user)
            return self.app(env, start_response)

        start_response_args = [None]
        input_proxy = InputProxy(env['wsgi.input'])
        env['wsgi.input'] = input_proxy

        def my_start_response(status, headers, exc_info=None):
            start_response_args[0] = (status, list(headers), exc_info)

        def iter_response(iterable):
            iterator = iter(iterable)
            try:
                chunk = next(iterator)
                while not chunk:
                    chunk = next(iterator)
            except StopIteration:
                chunk = ''

            if start_response_args[0]:
                start_response(*start_response_args[0])

            bytes_sent = 0
            try:
                while chunk:
                    bytes_sent += len(chunk)
                    yield chunk
                    chunk = next(iterator)
            finally:
                try:
                    self.publish_sample(env, account,
                                        input_proxy.bytes_received,
                                        bytes_sent)
                except Exception:
                    self.logger.exception('Failed to publish samples')

        try:
            iterable = self.app(env, my_start_response)
        except Exception:
            self.publish_sample(env, account, input_proxy.bytes_received, 0)
            raise
        else:
            return iter_response(iterable)

    def publish_sample(self, env, account, bytes_received, bytes_sent):
        timestamp = normalize_timestamp(time.time())
        sample_time = (float(
            timestamp) // self.sample_rate + 1) * self.sample_rate
        trans_id = env.get('swift.trans_id')
        tenant_id = env.get('HTTP_X_TENANT_ID')
        remote_addr = env.get('REMOTE_ADDR')

        # check if account information object is existed.
        if not self.swift_account(env, tenant_id):
            obj = 'account/%s' % account
            self.put_hidden_object(self.aggregate_account, tenant_id, obj)

        # recording account's storage usage data
        self.record_usage_data(env, tenant_id, account, sample_time)

        container = '%s_%s_%s' % (sample_time, tenant_id, account)

        obj = '%s/%d/%d/%s/%s' % (timestamp, bytes_received, bytes_sent,
                                  trans_id, remote_addr)
        self.put_hidden_object(self.sample_account, container, obj)

    def put_hidden_object(self, account, container, obj):
        hidden_path = '/%s/%s/%s' % (account, container, obj)
        self.logger.debug('put sample_path: %s' % hidden_path)
        part, nodes = self.container_ring.get_nodes(self.sample_account,
                                                    container)
        for node in nodes:
            ip = node['ip']
            port = node['port']
            dev = node['device']
            action_headers = dict()
            action_headers['user-agent'] = 'utilization'
            action_headers['X-Timestamp'] = normalize_timestamp(time.time())
            action_headers['referer'] = 'utilization-middleware'
            action_headers['x-size'] = '0'
            action_headers['x-content-type'] = "text/plain"
            action_headers['x-etag'] = 'd41d8cd98f00b204e9800998ecf8427e'

            conn = http_connect(ip, port, dev, part, 'PUT', hidden_path,
                                action_headers)
            response = conn.getresponse()
            response.read()


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""
    conf = global_conf.copy()
    conf.update(local_conf)

    def utilization_filter(app):
        return UtilizationMiddleware(app, conf)

    return utilization_filter

