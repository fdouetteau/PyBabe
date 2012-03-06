
import csv 
from collections import namedtuple
import itertools
import re
from timeparse import parse_date, parse_datetime
import tempfile
from zipfile import ZipFile 
import os
from subprocess import Popen, PIPE
from cStringIO import StringIO
import codecs
from charset import UTF8Recoder, UTF8RecoderWithCleanup, PrefixReader

class Babe(object):
    
    def pull_command(self, command, name, names=None, inp=None, utf8_cleanup = None, encoding=None):
        return PullCommand(command, name, names, inp, utf8_cleanup, encoding) 
        
    def pull(self, filename = None, stream = None, name = None, names = None, format=None, encoding=None, utf8_cleanup=False, **kwargs):
        fileExtension = None
        if filename: 
            fileBaseName, fileExtension = os.path.splitext(filename) 
            fileExtension = fileExtension.lower()
            if len(fileExtension) > 0:
                fileExtension = fileExtension[1:]
                    
        if not format and fileExtension:
            if fileExtension in ['xlsx', 'csv', 'tsv']:
                format = fileExtension 
            else: 
                raise Exception("Unable to guess format") 
        
        if not format: 
            raise Exception("Unable to guess format")
        
        if not format in ['xlsx', 'csv']:
            raise Exception('Unsupported format %s' % format)
        
        if stream:
            instream = stream
        else:
            instream = open(filename, 'rb') 
        
        if format == 'xlsx':
            from openpyxl import load_workbook
            wb = load_workbook(filename=instream, use_iterators=True)
            ws = wb.get_active_sheet()
            return ExcelPull(name, names, ws)
            
        if format == 'csv':
            return self._pull_stream(instream, name, names, utf8_cleanup, encoding)
        
        
        
    def _pull_stream(self, instream, name, names, utf8_cleanup, encoding):
        if not encoding:
            encoding = 'utf8'
            
        if utf8_cleanup: 
            instream = UTF8RecoderWithCleanup(instream, encoding)
        elif codecs.getreader(encoding)  != codecs.getreader('utf-8'):
            instream = UTF8Recoder(instream, encoding)
        else:
            pass
        
        sniff_read = instream.next()
        instream = PrefixReader(sniff_read, instream)
        #print type(sniff_read), sniff_read
        try:
            dialect = csv.Sniffer().sniff(sniff_read)
            if dialect.delimiter.isalpha():
                # http://bugs.python.org/issue2078
                return LinePull(name, names, instream)
            if sniff_read.endswith('\r\n'):
                dialect.lineterminator = '\r\n'
            else:
                dialect.lineterminator = '\n'
        except:
            raise Exception ()
        return CSVPull(name, names, instream, dialect)
  
    def map(self, column,  f):
        return Map(f, column, self)
        
    def head(self, n):
        """Keep the first n lines"""
        return Head(self, n)
        
    def augment(self, function, names, name=None):
        """
        Create a new stream that augment an existing stream by addind new colums to it
        names. The column names
        name. The new name for the augmented stream. 
        function. The function to calculate the augmented column. 
            function(row) should return a sequence of the new values to append [value1, value2]
        """
        return Augment(function, names, name, self)
        
    def multimap(self, d):
        return MultiMap(self, d)
        
    def typedetect(self):
        "Create a stream where integer/floats are automatically detected"
        return TypeDetect(self)
        
    def sort(self, key):
        "Return a sorted input according to key"
        return Sort(self, key)
        
    def groupkey(self, key, red, initial_value, group_key=None, keepOriginal=False):
        """Group all elements with equal value for group_key. 
        value = red(value, row[key]) is called for each row with equal value for group_key
        See 'group'  
        """
        kr = KeyReducer()
        kr.key = key
        kr.reduce = red
        kr.initial_value = initial_value
        return self.group(kr, group_key=group_key, keepOriginal=keepOriginal)

    def keynormalize(self, key):
        """Normalize a column name to a valid python identifier"""
        return '_'.join(re.findall(r'\w+',key))


    def group(self, reducer, group_key = None, keepOriginal=False):
        """Group all elements with equal value for key, assuming sorted input.
        reducer.begin_group() is called each time a new value for key 'key' is found
        reducer.row(row) is called on each row
        reducer.group_result() is called after the last row containing a equal value for that key.
        It shall return a new value to emit.  
        If keepOriginal is True, original lines will be kept in the output streamalongside grouped values.
        If key is None all keys are group together. new_group() and end_group() are called once."""
        if group_key is None:
            return GroupAll(self, reducer, keepOriginal)
        else:
            return Group(self, group_key, reducer, keepOriginal)
            
    def log(self, stream = None, filename=None):
        "Log intermediate content into a file, for debugging purpoposes"
        return Log(self, stream, filename)
        
            
    def push(self, filename=None, stream = None, format=None, encoding=None, protocol=None, compress=None, **kwargs):
        metainfo = None
        writer = None
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
            if fileExtension in ['xlsx', 'csv', 'tsv']:
                format = fileExtension 
            else: 
                raise Exception("Unable to guess format") 
                
        if not format: 
            raise Exception("Unable to guess format")
        
        if not format in ['xlsx', 'csv']:
            raise Exception('Unsupported format %s' % format) 
                    
        if compress: 
            compress_baseName, compress_fileExtension = os.path.splitext(compress) 
            compress_fileExtension = compress_fileExtension.lower()[1:]
            if compress_fileExtension in ['zip']: 
                compress_format = compress_fileExtension 
            else:
                raise Exception('Unknown exception format %s' % compress_format)
                
        if not protocol:
            protocol = 'file'
        
        if not (protocol in ['file', 'ftp']):
            raise Exception('Unsupported protocol %s' % protocol)

        ftp = None
        if protocol == 'ftp' and kwargs.get('ftp_early_check', True):  # Fail fast for FTP. 
            from ftplib import FTP
            ftp = FTP()
            ftp.connect(kwargs['host'], kwargs.get('port', None))
            ftp.login(kwargs.get('user', None), kwargs.get('password', None))
            ftp.quit()
            
        # If external protocol or compression, write to a temporary file. 
        if protocol is not "file" or compress:
            outstream = tempfile.NamedTemporaryFile()
            to_close.append(outstream)
        elif stream: 
            outstream = stream
        else: 
            outstream = open(filename, 'wb')
            to_close.append(outstream)
            
#        if format in ['csv', 'tsv']:
#            encoding = "utf8" if not encoding else encoding 
#            c = codecs.getwriter(encoding)
#            outstream = c(outstream)  
#            outstream = DebugStream(outstream)
#        else:
#           if encoding:
#               raise Exception('Invalid encoding %s for format %s' % (encoding, format)) 
        
        if format in ['csv', 'tsv']: 
            if not encoding:
                encoding = 'utf-8'
        else: 
            if encoding:
                raise Exception('Invalid encoding %s for format %s' % (encoding, format))
        
        # Actually write the file. 
        if format == 'xlsx':
            from openpyxl import Workbook
            wb = Workbook(optimized_write = True)
            ws = wb.create_sheet()
            for k in self:
                if isinstance(k, MetaInfo):
                    metainfo = k
                    ws.append(metainfo.names)
                else:
                    ws.append(list(k))
            wb.save(outstream)
        elif format == 'csv':
            for k in self: 
                if isinstance(k, MetaInfo):
                    metainfo = k
                    writer = csv.writer(outstream, metainfo.dialect)
                    writer.writerow([u.encode(encoding) if isinstance(u, unicode) else u for u in metainfo.names])
                else:
                    l = [u.encode(encoding) if isinstance(u, unicode) else u for u in k]
                    writer.writerow(l)
        outstream.flush()
        
        # Apply file compression
        if compress_format == "zip": 
            if protocol != 'file':
                compress_file = tempfile.NamedTemporaryFile()
            else:
                compress_file = compress
            with ZipFile(compress_file, 'w') as myzip:
                myzip.write(outstream.name, filename)
            filename = compress
            outstream.close()
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

class DebugStream(object):
    def __init__(self, stream):
        self.stream = stream 
    def write(self, object):
        print type(object), object
        self.stream.write(object)
        
        
class Head(Babe):
    def __init__(self, stream, n):
        self.stream = stream 
        self.n = n
    def __iter__(self):
        n = self.n
        for row in self.stream: 
            if isinstance(row, MetaInfo):
                count = 0 
            else: 
                if count >= n: 
                    break
                count = count + 1
            yield row

class Log(Babe):
    def __init__(self, stream, logstream, filename):
        self.stream = stream
        if logstream:
            self.logstream = logstream
            self.do_close = False
        else:
            self.logstream = open(filename, 'wb')
            self.do_close = True
    
    def __iter__(self):
        for row in self.stream:
            if isinstance(row, MetaInfo):
                writer = csv.writer(self.logstream, row.dialect)
                writer.writerow(row.names)
            else:
                writer.writerow(list(row))
            yield row
        if self.do_close:
            self.logstream.close()



class PullCommand(Babe):
    def __init__(self, command, name, names, inp, utf8_cleanup, encoding):
        self.command = command
        self.name = name
        self.inp = inp
        self.names = names 
        self.utf8_cleanup = utf8_cleanup
        self.encoding = encoding
    def __iter__(self):
        p = Popen(self.command, stdin=PIPE, stdout=PIPE, stderr=None)
        if self.inp:
            p.stdin.write(self.inp)
        p.stdin.close()
        i = self._pull_stream(p.stdout,self.name, self.names, self.utf8_cleanup, self.encoding)
        for k in i:
            yield k
        p.wait()
        if p.returncode != 0: 
            raise Exception("Mysql Process error ")
        
            
class Sort(Babe):
    def __init__(self, stream, key):
        self.stream = stream
        self.key = key
        self.buffer = []
    def __iter__(self):
        count = 0
        for elt in self.stream:
            if isinstance(elt, MetaInfo):
                yield elt
            else:
                self.buffer.append((getattr(elt, self.key), count, elt))
                count = count + 1
        self.buffer.sort()
        for (k, c, elt) in self.buffer:
            yield elt

class GroupAll(Babe):
    def __init__(self, stream, reducer, keepOriginal):
        self.stream = stream
        self.reducer = reducer
        self.keepOriginal = keepOriginal
    def __iter__(self):
        self.reducer.begin_group()
        for elt in self.stream:
            if isinstance(elt, MetaInfo):
                yield elt
            else:
                if self.keepOriginal:
                    yield elt
                self.reducer.row(elt)
        yield self.reducer.group_result()
            
class Group(Babe):
    def __init__(self, stream, key, reducer, keepOriginal):
        self.stream = stream
        self.key = key
        self.reducer = reducer
        self.keepOriginal = keepOriginal
    def __iter__(self):
        pk = None
        for elt in self.stream:
            if isinstance(elt, MetaInfo):
                yield elt
            else:
                if self.keepOriginal:
                    yield elt 
                k = getattr(elt, self.key)
                if (pk is not None) and not (pk == k):
                    yield self.reducer.group_result()
                    self.reducer.begin_group()
                    pk = k 
                    self.reducer.row(elt)
                else:
                    self.reducer.row(elt)
        if pk is not None:
            yield self.reducer.group_result()    
                        
class Augment(Babe):
    def __init__(self, function, names, name, stream):
        self.function = function
        self.names = names
        self.name = name
        self.stream = stream
    def __iter__(self):
        for k in self.stream: 
            if isinstance(k, MetaInfo):
                info = MetaInfo(names=k.names + self.names, name=self.name if self.name else k.name, dialect=k.dialect) 
                t = namedtuple(info.name, map(self.keynormalize, info.names))
                yield info
            else: 
                k2 = t._make(list(k) + self.function(k))
                yield k2 

class Map(Babe):
    def __init__(self, f, column, stream):
        self.f = f
        self.column = column 
        self.stream = stream 
    def __iter__(self):
        return itertools.imap(lambda elt : elt._replace(**{self.column : self.f(getattr(elt, self.column))}) if not isinstance(elt, MetaInfo) else elt,
               self.stream)
               

class MultiMap(Babe):
    def __init__(self, stream, d):
        self.stream = stream 
        self.d = d
    def map(self, elt):
        if isinstance(elt, MetaInfo):
            return elt
        m = {}
        for k in self.d:
            m[k] = self.d[k](getattr(elt, k))
        return elt._replace(**m) 
    def __iter__(self):
        return itertools.imap(self.map, self.stream)
               
class TypeDetect(Babe):
    
    patterns = [r'(?P<int>[0-9]+)', 
         r'(?P<float>[0-9]+\.[0-9]+)',
         r'(?P<date>\d{2,4}/\d\d/\d\d|\d\d/\d\d/\d\d{2,4})', 
         r'(?P<datetime>\d\d/\d\d/\d\d{2,4} \d{2}:\d{2})'
        ]
         
    
    pattern = re.compile('(' + '|'.join(patterns) + ')$')
    
    def __init__(self, stream):
        self.stream = stream
        self.d = {}
    def __iter__(self):
        return itertools.imap(self.filter, self.stream)
    def filter(self, elt):
        if isinstance(elt, MetaInfo):
            return elt
        else:
            self.d.clear()
            for t in elt._fields:
                v = getattr(elt, t)
                if not isinstance(v, basestring):
                    continue
                g = self.pattern.match(v)
                if g: 
                    if g.group('int'):
                        self.d[t] = int(v)
                    elif g.group('float'):
                        self.d[t] = float(v)
                    elif g.group('date'):
                        self.d[t] = parse_date(v)
                    elif g.group('datetime'):
                        self.d[t] = parse_datetime(v)
            if len(self.d) > 0:
                return elt._replace(**self.d)
            else:
                return elt
        
class LinePull(Babe):
    def __init__(self, name, names, stream):
        self.name = name
        self.names = names
        self.stream = stream
    def __iter__(self):
        if self.names:
            t = namedtuple(self.name, map(self.keynormalize, self.names))
            metainfo = MetaInfo(name=self.name, names=self.names)
            yield metainfo
        if not metainfo:
            row = self.stream.next()
            row = row.rstrip('\r\n')
            t = namedtuple(self.name, [self.keynormalize(self.sniff_read)])
            metainfo = MetaInfo(name=self.name, names=[row])
            yield metainfo
        for row in self.stream:
            yield t._make([row.rstrip('\r\n')])
            
class CSVPull(Babe):
    def __init__(self, name, names, stream, dialect):
        self.name = name
        self.stream = stream
        self.dialect = dialect
        self.names = names
    def __iter__(self):
        t = None
        reader = csv.reader(self.stream, self.dialect)        
        if self.names:
            names = self.names
        else: 
            names = reader.next()
        normalize_names = map(self.keynormalize, names)
        metainfo = MetaInfo(dialect=self.dialect, names=names)
        t = namedtuple(self.name, normalize_names)
        yield metainfo
        for row in reader:
            yield t._make([unicode(x, 'utf-8') for x in row])
            
class ExcelPull(Babe):
    def __init__(self, name, names, ws):
        self.name = name
        self.ws = ws
        self.names = names
    def __iter__(self):
        it = self.ws.iter_rows()
        names = None
        if self.names: 
            names = self.names
            yield MetaInfo(names = self.names)
        else:
            names_row = it.next()
            names = [cell.internal_value for cell in names_row]
            yield MetaInfo(names=names)
        t = namedtuple(self.name, map(self.keynormalize,names))
        for row in it: # it brings a new method: iter_rows()
            yield t._make(map(self.valuenormalize, row))
        
    def valuenormalize(self, cell):
        if cell.number_format == '0': 
            return int(cell.internal_value)
        else: 
            return cell.internal_value
        
class MetaInfo(object): 
    def __init__(self, dialect = None, name=None, names = None):
        self.dialect = dialect
        self.names = names
        self.name = name
        
class KeyReducer(object):
    def begin_group(self):
        self.value = self.initial_value
    def row(self, row):
        self.last_row = row
        self.value = self.reduce(self.value, getattr(row, self.key))
    def group_result(self):
        return self.last_row._replace(**{self.key: self.value})
