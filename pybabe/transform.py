
from base import BabeBase, MetaInfo, keynormalize
import itertools
from collections import namedtuple
      

      
def domap(stream, column, function):
    return itertools.imap(lambda elt : elt._replace(**{column : function(getattr(elt, column))}) if not isinstance(elt, MetaInfo) else elt,
           stream)
BabeBase.register("map", domap)


def augment(stream, function, names, name=None):
    """
    Create a new stream that augment an existing stream by addind new colums to it
    names. The column names
    name. The new name for the augmented stream. 
    function. The function to calculate the augmented column. 
        function(row) should return a sequence of the new values to append [value1, value2]
    """
    for k in stream: 
        if isinstance(k, MetaInfo):
            info = MetaInfo(names=k.names + names, name=name if name else k.name, dialect=k.dialect) 
            t = namedtuple(info.name, map(keynormalize, info.names))
            yield info
        else: 
            k2 = t._make(list(k) + function(k))
            yield k2 

BabeBase.register('augment', augment)


def head(stream, n):
    for row in stream: 
        if isinstance(row, MetaInfo):
            count = 0 
        else: 
            if count >= n: 
                break
            count = count + 1
        yield row
BabeBase.register('head', head)

def multimap(stream, d):
    def ddmap(elt):
        if isinstance(elt, MetaInfo):
            return elt
        m = {}
        for k in d:
            m[k] = d[k](getattr(elt, k))
        return elt._replace(**m) 
    return itertools.imap(ddmap, stream)
    
BabeBase.register('multimap', multimap)

def split(stream, column, separator):
    for row in stream:
        if isinstance(row, MetaInfo):
            yield row
        else:
            value = getattr(row, column)
            values = value.split(separator)
            for v in values:
                yield row._replace(**{column:v})
BabeBase.register('split',split)

def replace(stream, oldvalue, newvalue, column = None):
    buf = []
    for row in stream:
        if isinstance(row, MetaInfo):
            yield row
        else:
            del buf[:] 
            change = False 
            for v in row:
                if v == oldvalue: 
                    buf.append(newvalue)
                    change = True
                else:
                    buf.append(v)
            if change:
                yield row._make(buf)
            else: 
                yield row 
                
BabeBase.register('replace', replace)

