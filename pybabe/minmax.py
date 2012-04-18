
from base import StreamHeader, BabeBase
import heapq
import itertools 


class Guard(object):
    def __init__(self):
        self.metainfo = None
    def filter(self, elt):
        if isinstance(elt, StreamHeader):
            self.metainfo = elt
            return False
        else:
            return True

def minmaxN(stream, column, n, max=True):
    "Keep the n rows maximizing value for 'column'"
    itt = iter(stream)
    elt = itt.next()
    if not isinstance(elt, StreamHeader):
        raise Exception("Missing metainfo")
    yield elt 
    while True:
        g = Guard()
        it = itertools.takewhile(g.filter, itt)
        f = heapq.nlargest if max else heapq.nsmallest 
        for elt in f(n, it, key=lambda row : getattr(row, column)):
            yield elt
        if not g.metainfo:
            break
            
def maxN(stream, column, n):
    for k in minmaxN(stream, column, n, max=True):
        yield k

def minN(stream, column, n):
    for k in minmaxN(stream, column, n, max=False):
        yield k 
    
BabeBase.register('maxN', maxN)
BabeBase.register('minN', minN)

    