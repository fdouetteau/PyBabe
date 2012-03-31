

import re
import os
from collections import namedtuple
from subprocess import Popen, PIPE
import tempfile
import shutil
import ConfigParser

class MetaInfo(object):
    def __init__(self, name, names, primary_keys = None, dialect=None):
        self.dialect = dialect
        self.names = names
        self.name = name
        self.primary_keys = None
        if not self.name:
            self.name = '__'.join(map(MetaInfo.keynormalize, self.names))
        self.t = namedtuple(self.name, map(MetaInfo.keynormalize, self.names))

    @classmethod
    def keynormalize(cls, key):
        """Normalize a column name to a valid python identifier"""
        s = '_'.join(re.findall(r'\w+',key))
        if s.startswith('_'):
            return s[1:]
        else:
            return s



    def insert(self, name, names):
        return MetaInfo(name=name if name else self.name,
            names=self.names + names,
            dialect=self.dialect)

    def replace(self, name, names):
        return MetaInfo(name=name if name else self.name,
        names=names,
         dialect=self.dialect)

    def get_primary_identifier(self, row, linecount):
        """Retrieve a primary identifier associated with a row
        If primary key are defined, those are used
        """
        if self.primary_keys:
            return '-'.join([getattr(row, k) for k in self.primary_keys])
        else:
            return self.name + '_' + str(linecount)





class BabeBase(object):

    pullFormats = {}
    pullFormatsNeedSeek = {}
    pushFormats = {}
    pullExtensions = {}
    pushExtensions = {}
    pushCompressFormats = {}
    pushCompressExtensions = {}
    pushProtocols = {}
    pullCompressFormats = {}
    pullCompressFormatsNeedSeek = {}
    pullCompressExtensions = {}
    pullProtocols = {}
    config = None

    @classmethod
    def get_config_object(cls):
        if cls.config:
            return cls.config
        cls.config = ConfigParser.ConfigParser()
        cls.config.read([os.path.join(os.path.dirname(__file__),'pybabe.cfg'), os.path.expanduser('~/.pybabe.cfg')])
        return cls.config

    @classmethod
    def get_config(cls, section, key):
        config = cls.get_config_object()
        return config.get(section, key)
            
    @classmethod
    def has_config(cls, section, key):
        config = cls.get_config_object()
        return config.has_option(section, key)


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
    def addPullPlugin(cls, format, supportedExtensions, m, need_seek=False):
        cls.pullFormats[format] = m
        cls.pullFormatsNeedSeek[format] = need_seek
        for s in supportedExtensions:
            cls.pullExtensions[s] = format

    @classmethod
    def addPushPlugin(cls, format, supportedExtensions, m):
        cls.pushFormats[format] = m
        for s in supportedExtensions:
            cls.pushExtensions[s] = format
            
    @classmethod
    def addCompressPushPlugin(cls, format, supportedExtensions, m):
        cls.pushCompressFormats[format] = m
        for s in supportedExtensions:
            cls.pushCompressExtensions[s] = format
            
    @classmethod
    def addCompressPullPlugin(cls, format, supportedExtensions, get_list, uncompress, need_seek=True):
        cls.pullCompressFormatsNeedSeek[format] = need_seek
        cls.pullCompressFormats[format] = (get_list, uncompress)
        for s in supportedExtensions:
            cls.pullCompressExtensions[s] = format
            
    @classmethod
    def addProtocolPushPlugin(cls, protocol, m, early_check):
        cls.pushProtocols[protocol] = (early_check, m)  
        
    @classmethod
    def addProtocolPullPlugin(cls, protocol, m):
        cls.pullProtocols[protocol] = m
    
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
    if ext in BabeBase.pullCompressExtensions:
        return (BabeBase.pullCompressExtensions[ext], format)
    if format:
        if not format in BabeBase.pullFormats:
            raise Exception("Unsupported format %s" % format)
        return (None, format) 
    if ext in BabeBase.pullExtensions:
        return (compress_format, BabeBase.pullExtensions[ext])
    raise Exception("Unable to guess extension")
    
def pull(null_stream, filename = None, stream = None, command = None, compress_format = None, command_input = None, name = None, names = None, format=None, encoding=None, utf8_cleanup=False, **kwargs):
    fileExtension = None
    to_close = []
    
    # Guess format 

    (compress_format, format)  =  guess_format(compress_format, format, filename)

    
    if 'protocol' in kwargs:
        instream = BabeBase.pullProtocols[kwargs['protocol']](filename, **kwargs)
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


    if (compress_format and BabeBase.pullCompressFormatsNeedSeek[compress_format])  or BabeBase.pullFormatsNeedSeek[format]:
        if not hasattr(instream, 'seek'): 
            ## Create a temporary file
            tf = tempfile.NamedTemporaryFile()
            print "Creating temp file %s" % tf.name
            shutil.copyfileobj(instream, tf)
            tf.flush()
            tf.seek(0)
            instream = tf
            to_close.append(instream)

    if compress_format:
        (content_list, uncompress) = BabeBase.pullCompressFormats[compress_format]
        (compress_handle, namelist) = content_list(instream)
        if len(namelist) > 1:
            raise Exception("Too many file in archive. Only archive with one file supported")
        filename = namelist[0]
        (compress_format, format) = guess_format(None, format, filename)
        instream = uncompress(compress_handle, filename)
        to_close.append(instream)
        
    ## Parse high level 
    i = BabeBase.pullFormats[format](fileExtension, instream, name, names, encoding, utf8_cleanup, **kwargs)
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
        if fileExtension in BabeBase.pushExtensions:
            format = BabeBase.pushExtensions[fileExtension] 
        else: 
            raise Exception("Unable to guess format") 
            
    if not format: 
        raise Exception("Unable to guess format")
    
    if not format in BabeBase.pushFormats: 
        raise Exception('Unsupported format %s' % format) 
                
    if compress: 
        compress_baseName, compress_fileExtension = os.path.splitext(compress) 
        compress_fileExtension = compress_fileExtension.lower()[1:]
        if compress_fileExtension in BabeBase.pushCompressExtensions: 
            compress_format = BabeBase.pushCompressExtensions[compress_fileExtension] 
        else:
            raise Exception('Unknown exception format %s' % compress_format)
                
    if protocol and not (protocol in BabeBase.pushProtocols):
        raise Exception('Unsupported protocol %s' % protocol)

    if protocol and kwargs.get('protocol_early_check', True):
        early_check = BabeBase.pushProtocols[protocol][0]
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
    BabeBase.pushFormats[format](fileExtension, instream, outstream, encoding)
    outstream.flush()
    
    if compress_format:
        # Apply file compression. If output protocol, use a temporary file name 
        if protocol:
            compress_file = tempfile.NamedTemporaryFile()
        else:
            compress_file = compress
        BabeBase.pushCompressFormats[compress_format](compress_file, outstream.name, filename)
        outstream = compress_file
            
    # Apply protocol 
    if protocol:
        BabeBase.pushProtocols[protocol][1](outstream.name, filename, **kwargs)
    
    for s in to_close:
        s.close()

BabeBase.registerFinalMethod('push', push)
        
