
import codecs
from base import MetaInfo, BabeBase

def pull(format, stream, name, names, kwargs):    
    stream = codecs.getreader(kwargs.get('encoding', 'utf8'))(stream)

    if not names:
        names=['text']
    
    metainfo = MetaInfo(name=name, names=names)
    yield metainfo 
    
    for line in stream:
        yield metainfo.t._make([line])

def push(format, instream, outfile, encoding, **kwargs):
    outstream = codecs.getwriter(kwargs.get('encoding', 'utf8'))(outfile)
    for row in instream:
        if isinstance(row, MetaInfo):
            pass
        else:
            for cell in row: 
                outstream.write(cell)
    outstream.flush()

BabeBase.addPullPlugin('txt', ['txt'], pull)
BabeBase.addPushPlugin('txt', ['txt'], push)