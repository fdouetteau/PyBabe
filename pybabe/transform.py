
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