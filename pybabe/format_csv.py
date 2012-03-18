
from base import BabeBase, MetaInfo
import csv
from charset import UTF8Recoder, UTF8RecoderWithCleanup, PrefixReader, UnicodeCSVWriter
import codecs

def linepull(stream, name, names, dialect):
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

def pull(format, stream, name, names, encoding, utf8_cleanup, **kwargs):    
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
    dialect = csv.Sniffer().sniff(sniff_read)
    if sniff_read.endswith('\r\n'):
        dialect.lineterminator = '\r\n'
    else:
        dialect.lineterminator = '\n'
    if dialect.delimiter.isalpha():
        # http://bugs.python.org/issue2078
        for row in  linepull(stream, name, names, dialect):
            yield row 
        return 
    for row in csvpull(stream, name, names, dialect):
        yield row 
        
def push(format, instream, outfile, encoding):
    if not encoding:
        encoding = "utf8"
    for k in instream: 
        if isinstance(k, MetaInfo):
            metainfo = k
            writer = UnicodeCSVWriter(outfile, dialect=metainfo.dialect, encoding=encoding)
            writer.writerow(metainfo.names)
        else:
            writer.writerow(list(k))
    
BabeBase.addPullPlugin('csv', ['csv', 'tsv', 'txt'], pull)  
BabeBase.addPushPlugin('csv', ['csv', 'tsv', 'txt'], push)   


