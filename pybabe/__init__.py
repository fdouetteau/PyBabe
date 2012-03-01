
import csv 
from collections import namedtuple
import itertools
import re

class Babe(object):
    def pull(self, resource, name, format=None, **kwargs):
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
        return CSVPull(name, stream, dialect)
  
    def map(self, column,  f ):
        return Map(f, column, self)
        
    def typedetect(self):
        "Create a stream where integer/floats are automatically detected"
        return TypeDetect(self)
        
    def push(self, resource, format=None, **kwards):
        metainfo = None
        writer = None
        if hasattr(resource, 'write'): 
            outstream = resource
        elif isinstance(resource, str):
            outstream = open(resource, 'wb')
        else:
            raise Exception()
        for k in self: 
            if isinstance(k, MetaInfo):
                metainfo = k
                writer = csv.writer(outstream, metainfo.dialect)
                writer.writerow(metainfo.names)
            else:
                writer.writerow(list(k))
                
class Map(Babe):
    def __init__(self, f, column, stream):
        self.f = f
        self.column = column 
        self.stream = stream 
    def __iter__(self):
        return itertools.imap(
               lambda elt : elt._replace(**{self.column : self.f(getattr(elt, self.column))}) if not isinstance(elt, MetaInfo) else elt,
               self.stream)
               
class TypeDetect(Babe):
    pattern = re.compile(r'((?P<int>[0-9]+)|(?P<float>[0-9]+\.[0-9]+))$')
    
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
                g = self.pattern.match(v)
                if g: 
                    if g.group('int'):
                        self.d[t] = int(v)
                    elif g.group('float'):
                        self.d[t] = float(v)
            if len(self.d) > 0:
                return elt._replace(**self.d)
            else:
                return elt
        
class CSVPull(Babe):
    def __init__(self, name, stream, dialect):
        self.name = name
        self.stream = stream
        self.dialect = dialect
    def __iter__(self):
        r = csv.reader(self.stream, self.dialect)
        names  = r.next()
        names = [s.strip() for s in names] # Normalize names
        metainfo = MetaInfo()
        metainfo.dialect = self.dialect
        metainfo.names = names
        t = namedtuple(self.name, names)
        yield metainfo
        for row in r:
            yield t._make(row)
        
class MetaInfo(object): 
    pass
    
if __name__ == "__main__": 
    babe = Babe()
    babe.pull('../tests/test.csv', name='Test').typedetect().map('foo', lambda x : x + 30).push('../tests/test2.csv')
    
        
        