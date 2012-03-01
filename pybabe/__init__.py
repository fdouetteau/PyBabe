
import csv 
from collections import namedtuple
import itertools
import re
from timeparse import parse_date, parse_datetime

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
  
    def map(self, column,  f):
        return Map(f, column, self)
        
    def multimap(self, d):
        return MultiMap(self, d)
        
    def typedetect(self):
        "Create a stream where integer/floats are automatically detected"
        return TypeDetect(self)
        
    def sort(self, key):
        return Sort(self, key)
        
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
                
class Sort(Babe):
    def __init__(self, stream, key):
        self.stream = stream
        self.key = key
        self.buffer = []
    def __iter__(self):
        count = 0
        for elt in self.stream:
            if isinstance(elt, MetaInfo):
                yield elt
            else:
                self.buffer.append((getattr(elt, self.key), count, elt))
                count = count + 1
        self.buffer.sort()
        for (k, c, elt) in self.buffer:
            yield elt
            

class Map(Babe):
    def __init__(self, f, column, stream):
        self.f = f
        self.column = column 
        self.stream = stream 
    def __iter__(self):
        return itertools.imap(lambda elt : elt._replace(**{self.column : self.f(getattr(elt, self.column))}) if not isinstance(elt, MetaInfo) else elt,
               self.stream)
               

class MultiMap(Babe):
    def __init__(self, stream, d):
        self.stream = stream 
        self.d = d
    def map(self, elt):
        if isinstance(elt, MetaInfo):
            return elt
        m = {}
        for k in self.d:
            m[k] = self.d[k](getattr(elt, k))
        return elt._replace(**m) 
    def __iter__(self):
        return itertools.imap(self.map, self.stream)
               
class TypeDetect(Babe):
    
    patterns = [r'(?P<int>[0-9]+)', 
         r'(?P<float>[0-9]+\.[0-9]+)',
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
    a = babe.pull('../tests/test.csv', name='Test').typedetect()
    a.map('foo', lambda x : -x).multimap({'bar' : lambda x : x + 1, 'f' : lambda f : f / 2 }).sort('foo').push('../tests/test2.csv')
    
        
        