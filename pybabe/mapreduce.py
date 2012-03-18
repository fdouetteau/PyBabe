
from base import MetaInfo, BabeBase
from tempfile import TemporaryFile
import cPickle
import heapq
import itertools

def sort(stream, key):
    buf = []
    for elt in stream:
        if isinstance(elt, MetaInfo):
            yield elt
        else:
            buf.append(elt)
    buf.sort(key=lambda obj: getattr(obj, key))
    for elt in buf:
        yield elt
        
BabeBase.register('sort', sort)        

def sort_diskbased(stream, key, nsize=100000):
    buf = []
    files = []
    count = 0 
    t = None
    for elt in stream: 
        if isinstance(elt, MetaInfo):
            t = elt.t 
            yield elt
        else:
            buf.append(elt)
            count = count + 1
            if count % nsize == 0: 
                buf.sort(key=lambda obj: getattr(obj, key))
                f = TemporaryFile()
                for item in buf:
                    cPickle.dump((getattr(item, key), list(item)), f, cPickle.HIGHEST_PROTOCOL)
                f.flush()
                files.append(f)
                del buf[:]
    def iter_on_file(f):
        try:
            while True:
                (key, v) = cPickle.load(f)
                yield (key, t._make(v))
        except EOFError:
            f.close()
    buf.sort()
    iterables = [iter_on_file(f) for f in files] + [itertools.imap(lambda obj : (getattr(obj, key), obj), buf)]
    for (k, row) in  heapq.merge(*iterables):
        yield row 

BabeBase.register('sort_diskbased', sort_diskbased)
    
class Reducer(object):
    def __init__(self, f):
        self.f = f
        self.buf = []
    def begin_group(self, key):
        del self.buf[:]     
        self.key = key 
    def row(self, row):
        self.buf.append(row)
    def end_group(self, t):
        if self.key:
            elt =  self.f(self.key, self.buf)
        else:
            elt =  self.f(self.buf)
        if isinstance(elt, t):
            return  elt 
        else:
            return  t._make(elt)
            
def build_reducer(reducer):
    if hasattr(reducer, "begin_group"):
        return reducer
    else:
        return Reducer(reducer)
    
    
def groupBy(stream, key, reducer, assume_sorted=False, name = None, columns=None):
    """
GroupBy all values for a key. 
If reducer is a function, function(t, key, row_group) is called with an array of all rows matching the key value
        t is the expected return type
        key is the common key for the group. 
Otherwise can be a 'Reducer' object. 
    reducer.begin_group(key) will be called at the beginning of each grou
    reducer.row(row) is called on each row
    reducer.end_group(t) will be called at the end of each group and should return the resulting row or a list of rows of type t   
    """
    reducer = build_reducer(reducer)
    if not assume_sorted:
        stream = sort(stream, key)
    pk = None
    for elt in stream:
        if isinstance(elt, MetaInfo):
            if columns or name:
                metainfo = elt.replace(name=name, names=columns)
            else:
                metainfo = elt
            yield metainfo
        else:
            k = getattr(elt, key)
            if k == pk:
                reducer.row(elt)
            else:
                if pk is not None:
                    eg = reducer.end_group(metainfo.t)    
                    if isinstance(eg, list):
                        for e in eg:
                            yield e
                    else:
                        yield eg
                pk = k 
                reducer.begin_group(k)
                reducer.row(elt)
    if pk is not None:
        eg = reducer.end_group(metainfo.t)
        if isinstance(eg, list):
            for e in eg:
                yield e 
        else:
            yield eg 
            
BabeBase.register('groupBy', groupBy)
    
def groupAll(stream, reducer, name = None, columns = None):
    """
    Group all keys
reducer can either be a function or a reducer object
if a function, reducer(t, rows) will be called with all the rows as a parameter
if an object, reducer.begin_group(), reducer.row() and reducer.end_group() will be called
    """
    reducer = build_reducer(reducer)
    reducer.begin_group(None)
    for elt in stream:
        if isinstance(elt, MetaInfo):
            if name or columns:
                metainfo = elt.replace(name, columns)
            else:
                metainfo = elt
            yield metainfo
        else:
            reducer.row(elt)
    yield reducer.end_group(metainfo.t)
                        
BabeBase.register('groupAll', groupAll)