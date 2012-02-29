
import csv 
from collections import namedtuple
import itertools

class Babe(object):
    def pull(self, name, resource, format=None, **kwargs):
        ## Open File
        stream = None
        if hasattr(resource,'read'): 
            stream = resource
        elif isinstance(resource, str): 
            stream = open(resource, 'rb')
        sniff_read = stream.readline()
        stream.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sniff_read)
        except:
            raise Exception ()
        r = csv.reader(stream, dialect)
        names  = r.next()
        names = [s.strip() for s in names] # Normalize names
        metainfo = MetaInfo()
        metainfo.dialect = dialect
        metainfo.names = names
        t = namedtuple(name, names)
        yield metainfo
        for row in r:
            yield t._make(row)
            
    def map(self, f, column,  stream):
        return itertools.imap(
            lambda elt : elt._replace(**{column : f(getattr(elt, column))}) if not isinstance(elt, MetaInfo) else elt,
            stream)
        
    def push(self, resource, stream, format=None, **kwards):
        metainfo = None
        writer = None
        if hasattr(resource, 'write'): 
            outstream = resource
        elif isinstance(resource, str):
            outstream = open(resource, 'wb')
        else:
            raise Exception()
        for k in stream: 
            if isinstance(k, MetaInfo):
                metainfo = k
                writer = csv.writer(outstream, metainfo.dialect)
                writer.writerow(metainfo.names)
            else:
                writer.writerow(list(k))
                
        
class MetaInfo(object): 
    pass
    
if __name__ == "__main__": 
    babe = Babe()
    babe.push('test2.csv', babe.map(lambda x : int(x) + 10, 'foo',  babe.pull('Test', 'test.csv')))
    
        
        