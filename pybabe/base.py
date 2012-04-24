

import re
import os
from collections import namedtuple
from subprocess import Popen, PIPE
import tempfile
import shutil
import ConfigParser
import cPickle
from string import Template
from cStringIO import StringIO
import sys

def my_import(name):
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

class StreamMeta(object): 
    pass

class StreamHeader(StreamMeta):
    source = None
    typename = None
    fields = None
    partition = None
    primary_key = None
    t = None

    def __init__(self, fields, source=None, typename=None,  partition = None, primary_key = None, t = None, **kwargs):
        self.source = source
        self.typename = typename
        self.fields = fields
        self.partition = partition
        self.primary_key = primary_key
        if not self.typename: 
            self.typename = source
        if not self.typename:
            self.typename = '_'.join(map(StreamHeader.keynormalize, self.fields))
        self.t = t if t else namedtuple(self.typename, map(StreamHeader.keynormalize, self.fields))

    ## Some state to be define for metainfo pickling. 

    def as_dict(self):
        d = {}
        d.update(self.__dict__)
        del d['t']
        return d

    @staticmethod
    def from_dict(d):
        return StreamHeader(**d)



    @classmethod
    def keynormalize(cls, key):
        """Normalize a column name to a valid python identifier"""
        s = '_'.join(re.findall(r'\w+',key))
        if s.startswith('_'):
            s = s[1:]
        if s[0].isdigit(): 
            s = 'd_' + s
        return s

    def insert(self, typename, fields):
        return StreamHeader(
            typename=typename if typename else self.typename, 
            source = self.source, 
            partition=  self.partition,
            fields = self.fields + fields)

    def replace(self, typename = None, fields = None, partition=partition):
        return StreamHeader(typename=typename if typename else self.typename,
            fields=fields if fields else self.fields, 
            t = self.t if not fields or typename else None, 
            partition=partition if partition else self.partition,
            source = self.source)

    def get_stream_name(self): 
        return '_'.join(filter(None, [self.source, self.partition]))

    def get_primary_identifier(self, row, linecount):
        """Retrieve a primary identifier associated with a row
        If primary key are defined, those are used
        """
        if self.primary_key:
            return getattr(row, self.primary_key)
        else:
            return str(linecount)# TODO : add paritition? 

class StreamFooter(StreamMeta): 
    pass 

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
    def get_config_with_env(cls, section, key, kwargs, default=None): 
        if key in kwargs: 
            return kwargs[key]
        if cls.has_config(section,key):
            return cls.get_config(section, key)
        if os.getenv(key):
            return os.getenv(key)
        if default is not None: 
            return default
        raise Exception("Unable to locate key %s from section %s in args, config or env" % (key, section))
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
    


def pull(null_stream, **kwargs):
    fileExtension = None
    to_close = []

    mempath = None
    if kwargs.get('memoize', False): 
        memoize_directory = kwargs.get('memoize_directory', None)
        if not memoize_directory: 
            ## TODO: not portable 
            memoize_directory = "/tmp/pybabe-memoize-%s" % os.getenv('USER')
        if not os.path.exists(memoize_directory): 
            os.mkdir(memoize_directory)

        s = cPickle.dumps(kwargs)
        hashvalue = hash('3' + s)
        mempath = os.path.join(memoize_directory, str(hashvalue))
        if os.path.exists(mempath):
            f = open(mempath)
            try:
                metainfo = None
                while True:
                    a = cPickle.load(f)
                    if isinstance(a, list):
                        for v in a: 
                            yield metainfo.t._make(v)
                    elif isinstance(a, StreamFooter):
                        yield a 
                    else:
                        metainfo = StreamHeader.from_dict(a)
                        yield metainfo
            except EOFError:
                f.close()
                return 


    # Guess format 

    filename = kwargs.get('filename', None)
    stream = kwargs.get('stream', None)
    command = kwargs.get('command', None)
    compress_format = kwargs.get('compress_format', None)
    command_input = kwargs.get('command_input', None)
    format = kwargs.get('format', None)

    (compress_format, format)  =  guess_format(compress_format, format, filename)

    if 'protocol' in kwargs:
        instream = BabeBase.pullProtocols[kwargs['protocol']](filename, **kwargs)
        to_close.append(instream)
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



    if (compress_format and BabeBase.pullCompressFormatsNeedSeek[compress_format])  or (format and BabeBase.pullFormatsNeedSeek[format]):
        if not hasattr(instream, 'seek'): 
            ## Create a temporary file
            tf = tempfile.NamedTemporaryFile()
            shutil.copyfileobj(instream, tf)
            tf.flush()
            tf.seek(0)
            instream = tf
            to_close.append(instream)

    if compress_format:
        (content_list, uncompress) = BabeBase.pullCompressFormats[compress_format]
        (compress_handle, namelist) = content_list(instream, filename)
        if len(namelist) > 1:
            raise Exception("Too many file in archive. Only archive with one file supported")
        filename = namelist[0]
        (compress_format, format) = guess_format(None, format, filename)
        instream = uncompress(compress_handle, filename)
        to_close.append(instream)
        

    ## Parse high level 
    i = BabeBase.pullFormats[format](format=fileExtension, stream=instream, kwargs=kwargs)

    if kwargs.get('memoize', False):
        f = open(mempath, "w")
        buf = []
        for r in i:
            if isinstance(r, StreamHeader):
                cPickle.dump(map(list, buf), f, cPickle.HIGHEST_PROTOCOL)
                del buf[:]
                cPickle.dump(r.as_dict(), f, cPickle.HIGHEST_PROTOCOL)
            elif isinstance(r, StreamFooter):
                cPickle.dump(map(list, buf), f, cPickle.HIGHEST_PROTOCOL)
                del buf[:]
                cPickle.dump(r, f, cPickle.HIGHEST_PROTOCOL)
            else:
                buf.append(r)
                if len(buf) >= 1000:
                    cPickle.dump(map(list, buf), f, cPickle.HIGHEST_PROTOCOL)
                    del buf[:]
            yield r
        f.close()
    else:
        for r in i: 
            yield r 
        
    if command:
        p.wait()
        
    for s in to_close:
        s.close()
        
        
BabeBase.register('pull', pull)

def split_ext(filename):
    fileBaseName, fileExtension = os.path.splitext(filename) 
    fileExtension = fileExtension.lower()
    if len(fileExtension) > 0:
        fileExtension = fileExtension[1:]
    return (fileBaseName, fileExtension)

        
def push(instream, filename=None, filename_template = None, directory = None, stream = None, format=None, encoding=None, protocol=None, compress=None, stream_dict=None, **kwargs):
    outstream = None
    compress_format = None
    fileExtension = None
    fileBaseName = None
    to_close = []


    ## Guess format from file extensions .. 
    filename_for_guess = filename if filename else filename_template

    if filename_for_guess: 
        fileBaseName, fileExtension = split_ext(filename_for_guess) 

    if fileExtension in BabeBase.pushCompressExtensions:
        if not compress_format:
            compress_format = BabeBase.pushCompressExtensions[fileExtension]
        fileBaseName, fileExtension = split_ext(fileBaseName)

    if not format and fileExtension in BabeBase.pushExtensions:
        format = BabeBase.pushExtensions[fileExtension] 
            
    if not format: 
        format = "csv"
    
    if not format in BabeBase.pushFormats: 
        raise Exception('Unsupported format %s' % format) 
    if compress_format and not compress_format in BabeBase.pushCompressFormats:
        raise Exception('Unsupported compression format %s' % compress_format)
                
    if protocol and not (protocol in BabeBase.pushProtocols):
        raise Exception('Unsupported protocol %s' % protocol)

    if protocol and kwargs.get('protocol_early_check', True):
        early_check = BabeBase.pushProtocols[protocol][0]
        if early_check:
            early_check(**kwargs)


    it = iter(instream)
    while True:
        this_filename = None
        try: 
            header = it.next()
        except StopIteration: 
            break 

        if not filename and filename_template:
            this_filename = Template(filename_template).substitute(header.__dict__)

        if directory and filename:
            this_filename = os.path.join(directory, this_filename if this_filename else filename)

        if this_filename == None:
            this_filename = filename 

        # If external protocol or compression, write to a temporary file. 
        if protocol or compress_format:
            outstream = tempfile.NamedTemporaryFile()
            to_close.append(outstream)
        elif stream_dict != None: 
            n = filename if filename else header.get_stream_name()
            if not n  in stream_dict:
                stream_dict[n] = StringIO()
            outstream = stream_dict[n]
        elif stream: 
            outstream = stream
        else: 
            outstream = open(this_filename, 'wb')
            to_close.append(outstream)
            
        # Actually write the file. 
        BabeBase.pushFormats[format](format, header, it, outstream, encoding, **kwargs)
        outstream.flush()
        
        if compress_format:
            # Apply file compression. If output protocol, use a temporary file name 
            if protocol:
                n = tempfile.NamedTemporaryFile()
                compress_file = n.name
            else:
                compress_file = this_filename
            name_in_archive = os.path.splitext(os.path.basename(this_filename))[0] + '.' + format
            BabeBase.pushCompressFormats[compress_format](compress_file, outstream.name, name_in_archive)
            if protocol:
                outstream = n 
                
        # Apply protocol 
        if protocol:
            BabeBase.pushProtocols[protocol][1](outstream.name, this_filename, **kwargs)
        
        for s in to_close:
            s.close()

BabeBase.registerFinalMethod('push', push)
        
