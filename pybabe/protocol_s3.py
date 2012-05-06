
from base import BabeBase
from cStringIO import StringIO
import fnmatch 
def get_bucket(kwargs):
    from boto.s3.connection import S3Connection
    key_id = BabeBase.get_config_with_env('s3', 'AWS_ACCESS_KEY_ID', kwargs)
    access_key = BabeBase.get_config_with_env('s3', 'AWS_SECRET_ACCESS_KEY', kwargs)
    conn = S3Connection(key_id, access_key)
    bucket = conn.get_bucket(kwargs['bucket'])
    return bucket 
    
def push(filename_topush, filename_remote, **kwargs):
    bucket = get_bucket(kwargs)
    from boto.s3.key import Key
    k = Key(bucket)
    k.key = filename_remote
    k.set_contents_from_filename(filename_topush)

def get_key(bucket, filename):
    if filename.find('?') >= 0 or filename.find('*') >= 0:
        comp = filename.rsplit('/', 1)
        p  = comp[0] + '/' if len(comp) > 1 else ''
        pattern = comp[1] if len(comp) > 1 else comp[0]
        match = None
        for k in bucket.list(p):
            if fnmatch.fnmatch(k.name[len(p):], pattern):
                if match:
                    raise Exception("Multiple key matching pattern %s : %s and %s ", (filename, k.name, match.name))
                match = k
        if match:
            return match
        else: 
            raise Exception("No key matching pattern %s " % filename)
    else:
        return bucket.get_key(filename)

class ReadLineWrapper(object):
    "Overrride next to enumerate 'lines' instead of bytes "
    def __init__(self, obj):
        self.obj = obj
        self.it = self.doiter()
    def __iter__(self):
        return self.it
        
    def next(self):
        return self.it.next()
        
    def doiter(self):
        remaining = None
        for bytes in self.obj:
            if remaining:
                s = StringIO(remaining)
                s.write(bytes)
            else:
                s = StringIO(bytes)
            for line in s:
                if line.endswith('\n'):
                    yield line
                else:
                    remaining = line
        if remaining:
            yield remaining
            
    def read(self, size=0):
        return self.obj.read(size)
        
    def close(self):
        self.obj.close()
        
def pull(filename_remote, **kwargs):
    bucket = get_bucket(kwargs)
    key = get_key(bucket, filename_remote)
    if not key: 
        raise Exception('Filename %s does not exist on %s' % (filename_remote, str(bucket))) 
    return ReadLineWrapper(key)

BabeBase.addProtocolPushPlugin('s3', push, None)
BabeBase.addProtocolPullPlugin('s3', pull)
