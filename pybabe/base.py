

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
    
pullFormats = {}
pullExtensions = {}
pushFormats = {}
pushExtensions = {}
pushCompressFormats = {}
pushCompressExtensions = {}
pushProtocols = {}
pullCompressFormats = {}
pullCompressExtensions = {}
pullProtocols = {}

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
        pullFormats[format] = m
        for s in supportedExtensions:
            pullExtensions[s] = format
            
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
            
    @classmethod
    def addCompressPullPlugin(cls, format, supportedExtensions, get_list, uncompress):
        pullCompressFormats[format] = (get_list, uncompress)
        for s in supportedExtensions:
            pullCompressExtensions[s] = format
            
    @classmethod
    def addProtocolPushPlugin(cls, protocol, m, early_check):
        pushProtocols[protocol] = (early_check, m)  
        
    @classmethod
    def addProtocolPullPlugin(cls, protocol, m):
        pullProtocols[protocol] = m
    
def get_extension(filename):
    if not filename:
        return None
    fileBaseName, fileExtension = os.path.splitext(filename) 
    fileExtension = fileExtension.lower()
    if len(fileExtension) > 0:
        fileExtension = fileExtension[1:]
    return fileExtension
    
def guess_format(compress_format, format, filename):
    "Guess the format from the filename and provided metadata"
    if compress_format:
        return (compress_format, format)
    ext = get_extension(filename)
    if ext in pullCompressExtensions:
        return (pullCompressExtensions[ext], format)
    if format:
        if not format in pullFormats:
            raise Exception("Unsupported format %s" % format)
        return (None, format) 
    if ext in pullExtensions:
        return (compress_format, pullExtensions[ext])
    raise Exception("Unable to guess extension")
    
def pull(null_stream, filename = None, stream = None, command = None, compress_format = None, command_input = None, name = None, names = None, format=None, encoding=None, utf8_cleanup=False, **kwargs):
    fileExtension = None
    to_close = []
    
    # Guess format 
    (compress_format, format)  =  guess_format(compress_format, format, filename)
    
    if 'protocol' in kwargs:
        instream = pullProtocols[kwargs['protocol']](filename, **kwargs)
    # Open File
    elif stream:
        instream = stream
    elif command:
        p = Popen(command, stdin=PIPE, stdout=PIPE, stderr=None)
        if command_input:
            p.stdin.write(command_input)
        p.stdin.close()
        instream = p.stdout
    elif filename:
        instream = open(filename, 'rb') 
        to_close.append(instream)
    else:
        raise Exception("No input stream provided")  

    if compress_format:
        (content_list, uncompress) = pullCompressFormats[compress_format]
        (compress_handle, namelist) = content_list(instream)
        if len(namelist) > 1:
            raise Exception("Too many file in archive. Only archive with one file supported")
        filename = namelist[0]
        (compress_format, format) = guess_format(None, format, filename)
        instream = uncompress(compress_handle, filename)
        to_close.append(instream)
        
    ## Parse high level 
    i = pullFormats[format](fileExtension, instream, name, names, encoding, utf8_cleanup, **kwargs)
    for r in i: 
        yield r 
    
    if command:
        p.wait()
        
    for s in to_close:
        s.close()
        
        
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
                
    if protocol and not (protocol in pushProtocols):
        raise Exception('Unsupported protocol %s' % protocol)

    if protocol and kwargs.get('protocol_early_check', True):
        early_check = pushProtocols[protocol][0]
        if early_check:
            print "Early check"
            early_check(**kwargs)
        
    
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
    if protocol:
        pushProtocols[protocol][1](outstream.name, filename, **kwargs)
    
    for s in to_close:
        s.close()

BabeBase.registerFinalMethod('push', push)
        
def keynormalize(key):
    """Normalize a column name to a valid python identifier"""
    return '_'.join(re.findall(r'\w+',key))
