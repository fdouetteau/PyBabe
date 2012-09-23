
from base import BabeBase
import urllib


def push(filename_topush, filename_remote, **kwargs):
    from ftplib import FTP
    ftp = FTP()
    ftp.connect(kwargs['host'], kwargs.get('port', None))
    ftp.login(kwargs.get('user', None), kwargs.get('password', None))
    f = open(filename_topush, 'rb')
    ftp.storbinary('STOR %s' % filename_remote, f)
    f.close()
    ftp.quit()


def early_check(**kwargs):
    from ftplib import FTP
    ftp = FTP()
    ftp.connect(kwargs['host'], kwargs.get('port', None))
    ftp.login(kwargs.get('user', None), kwargs.get('password', None))
    ftp.quit()


def pull(filename_remote, **kwargs):
    host = kwargs['host']
    if 'port' in kwargs:
        host = host + ':' + str(kwargs['port'])
    if 'user' in kwargs:
        host = kwargs['user'] + ':' + kwargs['password'] + '@' + host
    return urllib.urlopen('ftp://%s/%s' % (host, filename_remote))

BabeBase.addProtocolPushPlugin('ftp', push, early_check)
BabeBase.addProtocolPullPlugin('ftp', pull)
