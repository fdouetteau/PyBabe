
from base import BabeBase, StreamHeader, StreamFooter
import csv
from charset import UTF8Recoder, UTF8RecoderWithCleanup, PrefixReader, UnicodeCSVWriter
import codecs
import logging 

log = logging.getLogger("csv")

def linepull(stream, dialect, kwargs):
    it = iter(stream)
    fields = kwargs.get('fields', None)
    if not fields: 
        fields = [it.next().rstrip('\r\n')] 
    metainfo = StreamHeader(**dict(kwargs, fields=fields))
    yield metainfo
    for row in it:
        yield metainfo.t._make([row.rstrip('\r\n')])
    yield StreamFooter()
            
def build_value(x, null_value):
    if x == null_value:
        return None
    else: 
        return unicode(x, "utf-8")

def csvpull(stream,  dialect, kwargs):
    reader = csv.reader(stream, dialect)        
    fields = kwargs.get('fields', None)
    null_value = kwargs.get('null_value', "")
    ignore_malformed = kwargs.get('ignore_bad_lines', False)
    if not fields:
        fields = reader.next()
    metainfo = StreamHeader(**dict(kwargs, fields=fields))
    yield metainfo
    for row in reader:
        try:
            yield metainfo.t._make([build_value(x, null_value) for x in row])
        except Exception, e:
            if ignore_malformed:
                log.warn("Malformed line: %s, %s" % (row, e))
            else:
                raise e
    yield StreamFooter()

def pull(format, stream,kwargs):                        
    if kwargs.get('utf8_cleanup', False): 
        stream = UTF8RecoderWithCleanup(stream, kwargs.get('encoding', 'utf-8'))
    elif codecs.getreader(kwargs.get('encoding', 'utf-8'))  != codecs.getreader('utf-8'):
        stream = UTF8Recoder(stream, kwargs.get('encoding', None))
    else:
        pass

    delimiter = kwargs.get('delimiter', None)
        
    sniff_read = stream.next()
    stream = PrefixReader(sniff_read, stream, linefilter=kwargs.get("linefilter", None))
    dialect = csv.Sniffer().sniff(sniff_read)
    if sniff_read.endswith('\r\n'):
        dialect.lineterminator = '\r\n'
    else:
        dialect.lineterminator = '\n'
    if dialect.delimiter.isalpha() and not delimiter:
        # http://bugs.python.org/issue2078
        for row in  linepull(stream,  dialect, kwargs):
            yield row 
        return 
    if delimiter:
        dialect.delimiter = delimiter
    for row in csvpull(stream,  dialect, kwargs):
        yield row 
        

class default_dialect(csv.Dialect):
    lineterminator = '\n'
    delimiter = ','
    doublequote = False
    escapechar = '\\'
    quoting = csv.QUOTE_MINIMAL
    quotechar = '"'

def push(format, metainfo, instream, outfile, encoding, delimiter=None, **kwargs):
    if not encoding:
        encoding = "utf8"
    dialect = kwargs.get('dialect', default_dialect) 
    if delimiter:
        dialect.delimiter = delimiter
    writer = UnicodeCSVWriter(outfile, dialect=dialect, encoding=encoding)
    writer.writerow(metainfo.fields)
    for k in instream: 
        if isinstance(k, StreamFooter):
            break
        else:
            writer.writerow(k)
    
BabeBase.addPullPlugin('csv', ['csv', 'tsv', 'txt'], pull)  
BabeBase.addPushPlugin('csv', ['csv', 'tsv', 'txt'], push)   


