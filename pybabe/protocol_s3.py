
from base import BabeBase
from cStringIO import StringIO
import fnmatch 
import os

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

def get_keys(bucket, filename):
    if filename.find('?') >= 0 or filename.find('*') >= 0:
        comp = filename.rsplit('/', 1)
        p  = comp[0] + '/' if len(comp) > 1 else ''
        pattern = comp[1] if len(comp) > 1 else comp[0]
        keys = [k for k in bucket.list(p) if fnmatch.fnmatch(k.name[len(p):], pattern)]
        if len(keys) == 0:
            raise Exception("No key matching pattern %s " % filename)
        return keys
    else:
        b = bucket.get_key(filename)
        if b:
            return [b]
        else:
            raise Exception("File not found %s" % filename)

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
    cache = BabeBase.get_config("s3", "cache", default=False)
    if cache: 
        default_cache_dir = "/tmp/pybabe-s3-cache-%s" % os.getenv('USER')
        cache_dir = BabeBase.get_config("s3", "cache_dir", default=default_cache_dir)
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
    keys = get_keys(bucket, filename_remote)
    files = []
    for key in keys:
        if cache: 
            f = os.path.join(cache_dir, os.path.basename(key.name) + "-" + key.etag.replace('"', ''))
            if os.path.exists(f): 
                files.append(open(f, "r"))
            else:
                key.get_contents_to_filename(f + ".tmp")
                os.rename(f + ".tmp", f)
                files.append(open(f, "r"))
        else:
            files.append(ReadLineWrapper(key))
    return files

BabeBase.addProtocolPushPlugin('s3', push, None)
BabeBase.addProtocolPullPlugin('s3', pull)
