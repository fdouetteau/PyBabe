
import codecs
from base import StreamHeader, BabeBase, StreamFooter

def pull(format, stream, kwargs):    
    stream = codecs.getreader(kwargs.get('encoding', 'utf8'))(stream)

    fields = kwargs.get('fields', ['text'])
    
    metainfo = StreamHeader(**dict(kwargs, fields=fields))
    yield metainfo 
    
    for line in stream:
        yield metainfo.t._make([line])
    yield StreamFooter()

def push(format, metainfo, instream, outfile, encoding, **kwargs):
    outstream = codecs.getwriter(kwargs.get('encoding', 'utf8'))(outfile)
    for row in instream:
        if isinstance(row, StreamFooter):
            break
        else:
            for cell in row: 
                outstream.write(cell)
    outstream.flush()

BabeBase.addPullPlugin('txt', ['txt'], pull)
BabeBase.addPushPlugin('txt', ['txt'], push)