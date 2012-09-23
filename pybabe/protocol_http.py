

from base import BabeBase
import urllib2


def build_host(kwargs):
    host = kwargs['host']
    if 'port' in kwargs:
        host = host + ':' + str(kwargs['port'])
    if 'user' in kwargs:
        host = kwargs['user'] + ':' + kwargs['password'] + '@' + host
    return host


def push(filename_topush, filename_remote, **kwargs):
    req = urllib2.Request()
    req.add_data
    host = build_host(kwargs)
    f = open(filename_topush, 'rb')
    urllib2.urlopen(url='%s://%s/%s' % (kwargs['protocol'], host, filename_remote), data=f)
    f.close()


def pull(filename_remote, **kwargs):
    host = build_host(kwargs)
    url = '%s://%s/%s' % (kwargs['protocol'], host, filename_remote)
    return urllib2.urlopen(url)

BabeBase.addProtocolPushPlugin('http', push, None)
BabeBase.addProtocolPullPlugin('http', pull)
BabeBase.addProtocolPushPlugin('https', push, None)
BabeBase.addProtocolPullPlugin('https', pull)
