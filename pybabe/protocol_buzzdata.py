

from base import BabeBase
import urllib2
import urllib
import json


def pull_buzz(stream, username, dataroom, uuid, **kwargs):
    url = 'https://buzzdata.com/api/%s/%s/%s/download_request' % (username, dataroom, uuid)
    if 'api_key' in kwargs:
        api_key = kwargs['api_key']
    elif BabeBase.get_config('buzzdata', 'api_key'):
        api_key = BabeBase.get_config('buzzdata', 'api_key')
    else:
        raise Exception('Missing api_key')
    data = urllib.urlencode([('api_key', api_key)])
    drequest = urllib2.urlopen(url, data).read()
    obj = json.loads(drequest)
    download_url = obj['download_request']['url']
    return urllib2.urlopen(download_url)

BabeBase.addProtocolPullPlugin('buzzdata', pull_buzz)
