

import re, os
from collections import namedtuple
from subprocess import Popen, PIPE
import tempfile


class MetaInfo(object): 
    def __init__(self, name, names, dialect = None):
        self.dialect = dialect
        self.names = names
        self.name = name
        self.t = namedtuple(self.name, map(keynormalize, self.names))
    
formats = {}
extensions = {}
pushFormats = {}
pushExtensions = {}
pushCompressFormats = {}
pushCompressExtensions = {}
                
class BabeBase(object):
    
    def __iter__(self):
        return self.m(self.stream, *self.v, **self.d)

    @classmethod
    def register(cls, name, m):
        # will return an iterator
        f = lambda self, *args, **kwargs : self.get_iterator(self, m, args, kwargs)
        setattr(cls, name, f)  
        
    @classmethod
    def registerFinalMethod(cls, name, m):
        setattr(cls, name, m)
        
    @classmethod
    def addPullPlugin(cls, format, supportedExtensions, m):
        formats[format] = m
        for s in supportedExtensions:
            extensions[s] = format
            
    @classmethod
    def addPushPlugin(cls, format, supportedExtensions, m):
        pushFormats[format] = m
        for s in supportedExtensions: 
            pushExtensions[s] = format
            
    @classmethod
    def addCompressPushPlugin(cls, format, supportedExtensions, m):
        pushCompressFormats[format] = m
        for s in supportedExtensions:
            pushCompressExtensions[s] = format
    
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
    i = formats[format](fileExtension, instream, name, names, encoding, utf8_cleanup, **kwargs)
    for r in i: 
        yield r 
    
    if command:
        p.wait()
    elif filename:
        instream.close()
        
        
BabeBase.register('pull', pull)
        
def push(instream, filename=None, stream = None, format=None, encoding=None, protocol=None, compress=None, **kwargs):
    outstream = None
    compress_format = None
    fileExtension = None
    to_close = []
    if filename: 
        fileBaseName, fileExtension = os.path.splitext(filename) 
        fileExtension = fileExtension.lower()
        if len(fileExtension) > 0:
            fileExtension = fileExtension[1:]
    
    if not format and fileExtension:
        if fileExtension in pushExtensions:
            format = pushExtensions[fileExtension] 
        else: 
            raise Exception("Unable to guess format") 
            
    if not format: 
        raise Exception("Unable to guess format")
    
    if not format in pushFormats: 
        raise Exception('Unsupported format %s' % format) 
                
    if compress: 
        compress_baseName, compress_fileExtension = os.path.splitext(compress) 
        compress_fileExtension = compress_fileExtension.lower()[1:]
        if compress_fileExtension in pushCompressExtensions: 
            compress_format = pushCompressExtensions[compress_fileExtension] 
        else:
            raise Exception('Unknown exception format %s' % compress_format)
                
    if protocol and not (protocol in ['ftp']):
        raise Exception('Unsupported protocol %s' % protocol)

    ftp = None
    if protocol == 'ftp' and kwargs.get('ftp_early_check', True):  # Fail fast for FTP. 
        from ftplib import FTP
        ftp = FTP()
        ftp.connect(kwargs['host'], kwargs.get('port', None))
        ftp.login(kwargs.get('user', None), kwargs.get('password', None))
        ftp.quit()
        
    
    # If external protocol or compression, write to a temporary file. 
    if protocol or compress:
        outstream = tempfile.NamedTemporaryFile()
        to_close.append(outstream)
    elif stream: 
        outstream = stream
    else: 
        outstream = open(filename, 'wb')
        to_close.append(outstream)
        
    # Actually write the file. 
    pushFormats[format](fileExtension, instream, outstream, encoding)
    outstream.flush()
    
    if compress_format:
        # Apply file compression. If output protocol, use a temporary file name 
        if protocol:
            compress_file = tempfile.NamedTemporaryFile()
        else:
            compress_file = compress
        pushCompressFormats[compress_format](compress_file, outstream.name, filename)
        outstream = compress_file
            
    # Apply protocol 
    if protocol == 'ftp': 
        from ftplib import FTP
        ftp = FTP()
        ftp.connect(kwargs['host'], kwargs.get('port', None))
        ftp.login(kwargs.get('user', None), kwargs.get('password', None))
        ftp.storbinary('STOR %s' % filename, open(outstream.name, 'rb'))
        ftp.quit()
    for s in to_close:
        s.close()

BabeBase.registerFinalMethod('push', push)
        
def keynormalize(key):
    """Normalize a column name to a valid python identifier"""
    return '_'.join(re.findall(r'\w+',key))
