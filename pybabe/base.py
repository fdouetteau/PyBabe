

import re
from collections import namedtuple

class MetaInfo(object): 
    def __init__(self, name, names, dialect = None):
        self.dialect = dialect
        self.names = names
        self.name = name
        self.t = namedtuple(self.name, map(keynormalize, self.names))
        
    
class BabeBase(object):
    
    def __iter__(self):
        return self.m(self.stream, *self.v, **self.d)
    
    @classmethod
    def register(cls, name, m):
        # will return an iterator
        f = lambda self, *args, **kwargs : self.get_iterator(self, m, args, kwargs)
        setattr(cls, name, f)    
        
def keynormalize(key):
    """Normalize a column name to a valid python identifier"""
    return '_'.join(re.findall(r'\w+',key))
