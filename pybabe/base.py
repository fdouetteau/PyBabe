

import re

class MetaInfo(object): 
    def __init__(self, dialect = None, name=None, names = None):
        self.dialect = dialect
        self.names = names
        self.name = name
    
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
