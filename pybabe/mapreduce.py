
from base import MetaInfo, BabeBase


def sort(stream, key):
    buf = []
    count = 0
    for elt in stream:
        if isinstance(elt, MetaInfo):
            yield elt
        else:
            buf.append((getattr(elt, key), count, elt))
            count = count + 1
    buf.sort()
    for (k, c, elt) in buf:
        yield elt
        
BabeBase.register('sort', sort)        

    
class KeyReducer(object):
    def begin_group(self):
        self.value = self.initial_value
    def row(self, row):
        self.last_row = row
        self.value = self.reduce(self.value, getattr(row, self.key))
    def group_result(self):
        return self.last_row._replace(**{self.key: self.value})
        
    
def groupkey(stream, key, red, initial_value, group_key=None, keepOriginal=False):
    """Group all elements with equal value for group_key. 
    value = red(value, row[key]) is called for each row with equal value for group_key
    See 'group'  
    """
    kr = KeyReducer()
    kr.key = key
    kr.reduce = red
    kr.initial_value = initial_value
    return group(stream, kr, group_key=group_key, keepOriginal=keepOriginal)

BabeBase.register('groupkey', groupkey)


def group(stream, reducer, group_key = None, keepOriginal=False):
    """Group all elements with equal value for key, assuming sorted input.
    reducer.begin_group() is called each time a new value for key 'key' is found
    reducer.row(row) is called on each row
    reducer.group_result() is called after the last row containing a equal value for that key.
    It shall return a new value to emit.  
    If keepOriginal is True, original lines will be kept in the output streamalongside grouped values.
    If key is None all keys are group together. new_group() and end_group() are called once."""
    if group_key is None:
        reducer.begin_group()
        for elt in stream:
            if isinstance(elt, MetaInfo):
                yield elt
            else:
                if keepOriginal:
                    yield elt
                reducer.row(elt)
        yield reducer.group_result()
    else:
        pk = None
        for elt in stream:
            if isinstance(elt, MetaInfo):
                yield elt
            else:
                if keepOriginal:
                    yield elt 
                k = getattr(elt, group_key)
                if (pk is not None) and not (pk == k):
                    yield reducer.group_result()
                    reducer.begin_group()
                    pk = k 
                    reducer.row(elt)
                else:
                    reducer.row(elt)
        if pk is not None:
            yield reducer.group_result()    
                        
BabeBase.register('group', group)