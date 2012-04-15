
from base import BabeBase, MetaInfo
import csv
from charset import UTF8Recoder, UTF8RecoderWithCleanup, PrefixReader, UnicodeCSVWriter
import codecs

def linepull(stream, name, names, dialect, kwargs):
    it = iter(stream)
    if names:
        metainfo = MetaInfo(name=name, names=names, dialect=dialect)
        yield metainfo
    else:
        row = it.next()
        row = row.rstrip('\r\n')
        metainfo = MetaInfo(name=name, names=[row], dialect=dialect)
        yield metainfo
    for row in it:
        yield metainfo.t._make([row.rstrip('\r\n')])
            
def csvpull(stream, name, names, dialect, kwargs):
    reader = csv.reader(stream, dialect)        
    if not names:
        names = reader.next()
    metainfo = MetaInfo(name=name, dialect=dialect, names=names, primary_keys=kwargs.get('primary_key', kwargs.get('primary_keys', None)))
    yield metainfo
    for row in reader:
        if name == 'ls': 
            print row
        yield metainfo.t._make([unicode(x, 'utf-8') for x in row])

def pull(format, stream, name, names, kwargs):                        
    if kwargs.get('utf8_cleanup', False): 
        stream = UTF8RecoderWithCleanup(stream, kwargs.get('encoding', 'utf-8'))
    elif codecs.getreader(kwargs.get('encoding', 'utf-8'))  != codecs.getreader('utf-8'):
        stream = UTF8Recoder(stream, kwargs.get('encoding', None))
    else:
        pass
        
    sniff_read = stream.next()
    stream = PrefixReader(sniff_read, stream)
    dialect = csv.Sniffer().sniff(sniff_read)
    if sniff_read.endswith('\r\n'):
        dialect.lineterminator = '\r\n'
    else:
        dialect.lineterminator = '\n'
    if dialect.delimiter.isalpha():
        # http://bugs.python.org/issue2078
        for row in  linepull(stream, name, names, dialect, kwargs):
            yield row 
        return 
    for row in csvpull(stream, name, names, dialect, kwargs):
        yield row 
        

class default_dialect(csv.Dialect):
    lineterminator = '\n'
    delimiter = ','
    doublequote = False
    escapechar = '\\'
    quoting = csv.QUOTE_MINIMAL
    quotechar = '"'

def push(format, instream, outfile, encoding, delimiter=None, **kwargs):
    if not encoding:
        encoding = "utf8"
    for k in instream: 
        if isinstance(k, MetaInfo):
            metainfo = k
            dialect = metainfo.dialect if metainfo.dialect else default_dialect() 
            if delimiter:
                dialect.delimiter = delimiter
            writer = UnicodeCSVWriter(outfile, dialect=dialect, encoding=encoding)
            writer.writerow(metainfo.names)
        else:
            writer.writerow(k)
    
BabeBase.addPullPlugin('csv', ['csv', 'tsv', 'txt'], pull)  
BabeBase.addPushPlugin('csv', ['csv', 'tsv', 'txt'], push)   


