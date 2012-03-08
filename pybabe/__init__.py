
import csv 
import itertools
import re
from timeparse import parse_date, parse_datetime
import tempfile
from zipfile import ZipFile, ZIP_DEFLATED
import os
from subprocess import Popen, PIPE
import codecs
from charset import UTF8Recoder, UTF8RecoderWithCleanup, PrefixReader, UnicodeCSVWriter 
import transform, mapreduce
from base import BabeBase, MetaInfo, keynormalize
        
only_to_load_1 = transform
only_to_load_2 = mapreduce

    
        
class Babe(BabeBase):
    
    def keynormalize(self, k):
        return keynormalize(k)
    
    def get_iterator(self, stream, m, v, d):
        b = Babe()
        b.stream = stream
        b.m = m
        b.v = v 
        b.d = d 
        return b
        
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
  
        
    
    def typedetect(self):
        "Create a stream where integer/floats are automatically detected"
        return TypeDetect(self)
        
            
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
                    writer = UnicodeCSVWriter(outstream, dialect=metainfo.dialect, encoding=encoding)
                    writer.writerow(metainfo.names)
                else:
                    writer.writerow(list(k))
        outstream.flush()
        
        # Apply file compression
        if compress_format == "zip": 
            if protocol != 'file':
                compress_file = tempfile.NamedTemporaryFile()
            else:
                compress_file = compress
            myzip = ZipFile(compress_file, 'w', ZIP_DEFLATED)
            myzip.write(outstream.name, filename)
            myzip.close()
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
        
               

               
class TypeDetect(Babe):
    
    patterns = [r'(?P<int>-?[0-9]+)', 
         r'(?P<float>-?[0-9]+\.[0-9]+)',
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
            metainfo = MetaInfo(name=self.name, names=self.names)
            yield metainfo
        if not metainfo:
            row = self.stream.next()
            row = row.rstrip('\r\n')
            metainfo = MetaInfo(name=self.name, names=[row])
            yield metainfo
        for row in self.stream:
            yield metainfo.t._make([row.rstrip('\r\n')])
            
class CSVPull(Babe):
    def __init__(self, name, names, stream, dialect):
        self.name = name
        self.stream = stream
        self.dialect = dialect
        self.names = names
    def __iter__(self):
        reader = csv.reader(self.stream, self.dialect)        
        if self.names:
            names = self.names
        else: 
            names = reader.next()
        metainfo = MetaInfo(name=self.name, dialect=self.dialect, names=names)
        yield metainfo
        for row in reader:
            yield metainfo.t._make([unicode(x, 'utf-8') for x in row])
            
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
            metainfo =  MetaInfo(name=self.name, names=names)
            yield metainfo
        for row in it: # it brings a new method: iter_rows()
            yield metainfo.t._make(map(self.valuenormalize, row))
        
    def valuenormalize(self, cell):
        if cell.number_format == '0': 
            return int(cell.internal_value)
        else: 
            return cell.internal_value
        

