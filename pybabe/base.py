

import re, os
from collections import namedtuple
from subprocess import Popen, PIPE

class MetaInfo(object): 
    def __init__(self, name, names, dialect = None):
        self.dialect = dialect
        self.names = names
        self.name = name
        self.t = namedtuple(self.name, map(keynormalize, self.names))
    
formats = {}
extensions = {}
                
class BabeBase(object):
    
    def __iter__(self):
        return self.m(self.stream, *self.v, **self.d)

    @classmethod
    def register(cls, name, m):
        # will return an iterator
        f = lambda self, *args, **kwargs : self.get_iterator(self, m, args, kwargs)
        setattr(cls, name, f)  
        
    @classmethod
    def addPullPlugin(cls, format, supportedExtensions, m):
        formats[format] = m
        for s in supportedExtensions:
            extensions[s] = format
    
def pull(null_stream, filename = None, stream = None, command = None, command_input = None, name = None, names = None, format=None, encoding=None, utf8_cleanup=False, **kwargs):
    fileExtension = None
    if filename: 
        fileBaseName, fileExtension = os.path.splitext(filename) 
        fileExtension = fileExtension.lower()
        if len(fileExtension) > 0:
            fileExtension = fileExtension[1:]
                
    if not format and fileExtension:
        if fileExtension in extensions:
            format = extensions[fileExtension]
        else: 
            raise Exception("Unable to guess format") 
    
    if not format: 
        raise Exception("Unable to guess format")
    
    if not format in formats:
        raise Exception('Unsupported format %s' % format)
    
    if stream:
        instream = stream
    elif command:
        p = Popen(command, stdin=PIPE, stdout=PIPE, stderr=None)
        if command_input:
            p.stdin.write(command_input)
        p.stdin.close()
        instream = p.stdout
    elif filename:
        instream = open(filename, 'rb') 
    else:
        raise Exception("No input stream provided")  
    i = formats[format](format, instream, name, names, encoding, utf8_cleanup, **kwargs)
    for r in i: 
        yield r 
    
    if command:
        p.wait()
    elif filename:
        instream.close()
        
        
BabeBase.register('pull', pull)
        
def keynormalize(key):
    """Normalize a column name to a valid python identifier"""
    return '_'.join(re.findall(r'\w+',key))
