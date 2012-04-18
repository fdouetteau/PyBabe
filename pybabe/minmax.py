
from base import StreamHeader, BabeBase, StreamFooter
import heapq
import itertools 


class Guard(object):
    def __init__(self):
        self.footer = None
    def filter(self, elt):
        if isinstance(elt, StreamFooter):
            self.footer = elt
            return False
        else:
            return True

def minmaxN(stream, column, n, max=True):
    "Keep the n rows maximizing value for 'column' for each stream"
    itt = iter(stream)
    while True:
        elt = itt.next()
        if not isinstance(elt, StreamHeader):
            raise Exception("Missing metainfo")
        yield elt         
        g = Guard()
        it = itertools.takewhile(g.filter, itt)
        f = heapq.nlargest if max else heapq.nsmallest 
        for elt in f(n, it, key=lambda row : getattr(row, column)):
            yield elt
        yield g.footer
            
def maxN(stream, column, n):
    for k in minmaxN(stream, column, n, max=True):
        yield k

def minN(stream, column, n):
    for k in minmaxN(stream, column, n, max=False):
        yield k 
    
BabeBase.register('maxN', maxN)
BabeBase.register('minN', minN)

    