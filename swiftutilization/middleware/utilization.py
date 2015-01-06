# coding=utf-8

import json
import time
from datetime import datetime
from swift.common.swob import Request, Response
from swift.common.utils import get_logger, InputProxy, \
    normalize_timestamp, get_remote_client
from swift.common.wsgi import make_pre_authed_request
from swift.common.ring import Ring
from swift.common.bufferedhttp import http_connect


class UtilizationMiddleware(object):
    def __init__(self, app, conf, *args, **kwargs):
        self.app = app
        self.conf = conf
        self.sample_account = '.transfer'
        self.aggregate_account = '.utilization'
        self.logger = get_logger(self.conf, log_route='utilization')
        self.reseller_prefix = conf.get('reseller_prefix', 'AUTH').strip()
        if self.reseller_prefix and self.reseller_prefix[-1] != '_':
            self.reseller_prefix += '_'
        self.container_ring = Ring('/etc/swift', ring_name='container')
        self.sample_rate = 300

    def GET(self, req):
        # /api/v0/utilization/[transfer|storage|/                  - total
        # /api/v0/utilization/[transfer|storage]/<account>         - single
        # /api/v0/utilization/[transfer|storage]/<account>/detail  - detail
        try:
            api, version, cate, utype, account, detail = req.split_path(4, 6, True)
        except ValueError:
            return Response(request=req, status="404 Not Found",
                            body='Invalid path: %s' % req.path,
                            content_type="text/plain")

        if cate.lower() != 'utilization' or \
                utype.lower() not in ['transfer', 'storage']:
            return Response(request=req, status="404 Not Found",
                            body='Invalid path: %s' % req.path,
                            content_type="text/plain")

        start = req.params.get('start')
        end = req.params.get('end')
        if end is None:
            end = datetime.utcfromtimestamp(time.time()).isoformat()

        if account:
            if utype.lower() == 'transfer':
                content = {'bytes_in':0, 'bytes_out':0, 'req_count':0}
                content['account'] = account
                content['resource_uri'] = \
                    '/api/%s/utilization/transfer/%s/' % (version, account)
                content['start'] = start
                content['end'] = end

                path = '/v1/%s/%s/?prefix=transfer/' % \
                       (self.aggregate_account, account)
                req = make_pre_authed_request(req.environ.copy(), 'GET', path)
                resp = req.get_response(self.app)
                if resp.status_int // 100 == 2:
                    for o in resp.body.strip().split('\n'):
                        self.logger.debug('transfer sample: %s' % o)
                        data = o.split('/')[2].split('_')
                        content['bytes_in'] += int(data[0])
                        content['bytes_out'] += int(data[1])
                        content['req_count'] += int(data[2])
                        if not content['start']:
                            content['start'] = o.split('/')[1]
                        content['end']
            if utype.lower() == 'storage':
                content = {'container_count':0, 'object_count':0, 'bytes_used':0}
                content['account'] = account
                content['resource_uri'] = \
                    '/api/%s/utilization/transfer/%s/' % (version, account)
                content['start'] = start
                content['end'] = end

                path = '/v1/%s/%s/?prefix=usage/' % \
                       (self.aggregate_account, account)
                req = make_pre_authed_request(req.environ.copy(), 'GET', path)
                resp = req.get_response(self.app)
                if resp.status_int // 100 == 2:
                    for o in resp.body.strip().split('\n'):
                        self.logger.debug('usage sample: %s' % o)
                        data = o.split('/')[2].split('_')
                        content['container_count'] = int(data[0])
                        content['object_count'] = int(data[1])
                        content['bytes_used'] = int(data[2])
                        if not content['start']:
                            content['start'] = o.split('/')[1]
                        content['end']


        if content is not None:
            return Response(request=req, body=json.dumps(content),
                            content_type="application/json")
        else:
            return Response(request=req, status="500 Server Error",
                            body="Internal server error.",
                            content_type="text/plain")

    def __call__(self, env, start_response):
        self.logger.debug('Calling Utilization Middleware')

        req = Request(env)
        if req.path.startswith('/api/'):
            return self.GET(req)(env, start_response)

        try:
            version, account, container, obj = req.split_path(2, 4, True)
        except ValueError:
            return self.app(env, start_response)

        remote_user = env.get('REMOTE_USER')
        if not remote_user or (isinstance(remote_user, basestring) and \
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
        container = '%s_%s' % (account, (float(timestamp)//self.sample_rate+1)*self.sample_rate)

        '''
        REMOTE_USER samples
        # swauth
        env['REMOTE_USER'] = 'test:tester,test,AUTH_21059546-6d00-4b08-898b-1f727184af50'
        env['HTTP_X_USER_ID'] = None
        # keystone
        env['REMOTE_USER'] = (u'99a06db6365d4887ba636ff4b3da0f',u'lgcloud')
        env['HTTP_X_USER_ID'] = c71d8b70c51745c1aca49e06b5cc455b
        '''
        user_id = env.get('HTTP_X_USER_ID')
        if not user_id:
            user_id = env.get('REMOTE_USER').split(':')[1]
        client_ip = get_remote_client(Request(env))
        trans_id = env.get('swift.trans_id')
        obj = '%s/%s/%d_%d/%s/%s' % (timestamp, user_id, bytes_received,
                                     bytes_sent, client_ip, trans_id)

        hidden_path = '/%s/%s/%s' % (self.sample_account, container, obj)
        self.logger.debug('put sample_path: %s' % hidden_path)
        part, nodes = self.container_ring.get_nodes(self.sample_account, container)
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

