
from base import BabeBase

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
    
BabeBase.addProtocolPushPlugin('ftp', push, early_check)
