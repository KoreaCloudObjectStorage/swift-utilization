# coding=utf-8

import calendar
import json
import time
from datetime import datetime
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
        self.sample_rate = 600

    def swift_account(self, env, tenant_id):
        path = '/v1/%s/%s?format=json&prefix=account/' \
               % (self.aggregate_account, tenant_id)
        req = make_pre_authed_request(env, 'GET', path)
        resp = req.get_response(self.app)
        if resp.status_int == 404:
            return None
        return json.loads(resp.body)[0]['name'].split('/')[1]

    def check_api_call(self, env):
        if env['REQUEST_METHOD'] != 'GET' and 'RAW_PATH_INFO' not in env:
            return False

        if env['RAW_PATH_INFO'] != '/api/v1/metering':
            return False
        return True

    def iso8601_to_timestamp(self, strtime):
        return calendar.timegm(datetime.strptime(strtime,"%Y-%m-%dT%H:%M:%S")
                               .timetuple())

    def iter_objects(self, env, path, prefix, marker, end, count):
        path_with_params = '%s?format=json&prefix=%s' % (path, prefix)
        seg = ''
        force_break = False
        while True:
            l = 1000 if count > 1000 else count
            count -= 1000
            rpath = path_with_params + ('&marker=%s' % marker) + (
                '&limit=%d' % l)
            req = make_pre_authed_request(env, 'GET', rpath)
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

    def retrive_utilization_data(self, env, tenant_id, start, end, count):
        path = '/v1/%s/%s' % (self.aggregate_account, tenant_id)
        data = {}
        data['transfer'] = {}
        data['utilization'] = {}
        marker = 'transfer/%d' % start
        data['transfer']['bytes_in'] = 0
        data['transfer']['bytes_out'] = 0
        data['transfer']['req_count'] = 0
        data['utilization']['container_count'] = 0
        data['utilization']['object_count'] = 0
        data['utilization']['bytes_used'] = 0
        for o in self.iter_objects(env, path, 'transfer/', marker, end, count):
            bytes_recv, bytes_sent, req_cnt = o.split('/')[2].split('_')
            data['transfer']['bytes_in'] += int(bytes_recv)
            data['transfer']['bytes_out'] += int(bytes_sent)
            data['transfer']['req_count'] += int(req_cnt)

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

        #start time is "rounded down"
        start_ts = (self.iso8601_to_timestamp(start) // 3600) * 3600
        # end time is "rounded up"
        end_ts = (self.iso8601_to_timestamp(end) // 3600 + 1) * 3600
        objsize = (end_ts - start_ts) / self.sample_rate

        if start_ts > end_ts:
            return Response(status="400 Bad Request",
                            content_type="text/plain",
                            body="start time must be before the end time")

        content = self.retrive_utilization_data(req.environ.copy(), tenant_id,
                                                start_ts, end_ts, objsize)
        if content is None:
            return Response(request=req, status="500 Server Error",
                            body="Internal server error.",
                            content_type="text/plain")

        content['period_start'] = start
        content['period_end'] = end
        content['tenant_id'] = tenant_id
        content['swift_account'] = self.swift_account(req.environ.copy(),
                                                      tenant_id)
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
                    self.publish_sample(env, account, input_proxy.bytes_received,
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
        sample_time = (float(timestamp)//self.sample_rate+1)*self.sample_rate
        trans_id = env.get('swift.trans_id')
        tenant_id = env.get('HTTP_X_TENANT_ID')

        # check if account informatin object is existed.
        if not self.swift_account(env, tenant_id):
            obj = 'account/%s' % account
            self.put_hidden_object(self.aggregate_account, tenant_id, obj)

        container = '%s_%s_%s' % (sample_time, tenant_id, account)

        obj = '%s/%d/%d/%s' % (timestamp, bytes_received, bytes_sent, trans_id)
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

