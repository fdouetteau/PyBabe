
import codecs
from base import MetaInfo, BabeBase

def pull(format, stream, name, names, encoding, utf8_cleanup, **kwargs):    
    if not encoding:
          encoding = 'utf8'
    stream = codecs.getreader(encoding)(stream)

    if not names:
        names=['text']
    
    metainfo = MetaInfo(name=name, names=names)
    yield metainfo 
    
    for line in stream:
        yield metainfo.t._make([line])

def push(format, instream, outfile, encoding):
    if not encoding:
        encoding = "utf8"
    outstream = codecs.getwriter(encoding)(outfile)
    for row in instream:
        if isinstance(row, MetaInfo):
            pass
        else:
            for cell in row: 
                outstream.write(cell)
    outstream.flush()

BabeBase.addPullPlugin('txt', ['txt'], pull)
BabeBase.addPushPlugin('txt', ['txt'], push)