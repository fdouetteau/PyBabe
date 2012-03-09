
from base import BabeBase, MetaInfo
import csv
from charset import UTF8Recoder, UTF8RecoderWithCleanup, PrefixReader
import codecs

def linepull(stream, name, names):
    if names:
        metainfo = MetaInfo(name=name, names=names)
        yield metainfo
    if not metainfo:
        row = stream.next()
        row = row.rstrip('\r\n')
        metainfo = MetaInfo(name=name, names=[row])
        yield metainfo
    for row in stream:
        yield metainfo.t._make([row.rstrip('\r\n')])
            
def csvpull(stream, name, names, dialect):
    reader = csv.reader(stream, dialect)        
    if not names:
        names = reader.next()
    metainfo = MetaInfo(name=name, dialect=dialect, names=names)
    yield metainfo
    for row in reader:
        if name == 'ls': 
            print row
        yield metainfo.t._make([unicode(x, 'utf-8') for x in row])

def pull(format, stream, name, names, encoding, utf8_cleanup):    
    if not encoding:
        encoding = 'utf8'
                    
    if utf8_cleanup: 
        stream = UTF8RecoderWithCleanup(stream, encoding)
    elif codecs.getreader(encoding)  != codecs.getreader('utf-8'):
        stream = UTF8Recoder(stream, encoding)
    else:
        pass
        
    sniff_read = stream.next()
    stream = PrefixReader(sniff_read, stream)
    try:
        dialect = csv.Sniffer().sniff(sniff_read)
        if sniff_read.endswith('\r\n'):
            dialect.lineterminator = '\r\n'
        else:
            dialect.lineterminator = '\n'
        if dialect.delimiter.isalpha():
            # http://bugs.python.org/issue2078
            for row in  linepull(stream, name, names):
                yield row 
            return 
    except:
        raise Exception ()
    for row in csvpull(stream, name, names, dialect):
        yield row 

BabeBase.addPullPlugin('csv', ['csv', 'tsv', 'txt'], pull)  
        
