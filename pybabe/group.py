from base import StreamHeader, BabeBase, StreamFooter
from pybabe.sort import sort


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
            elt = self.f(self.key, self.buf)
        else:
            elt = self.f(self.buf)
        if isinstance(elt, t):
            return elt
        else:
            return t._make(elt)


def build_reducer(reducer):
    if hasattr(reducer, "begin_group"):
        return reducer
    else:
        return Reducer(reducer)


def group(stream, key, reducer,
		assume_sorted=False, typename=None, fields=None):
    """
GroupBy all values for a key.
If reducer is a function, function(t, key, row_group) is called with an array of all rows matching the key value
        t is the expected return type
        key is the common key for the group.
Otherwise can be a 'Reducer' object.
    reducer.begin_group(key) will be called at the beginning of each grou
    reducer.row(row) is called on each row
    reducer.end_group(t) will be called at the end of each group
    and should return the resulting row or a list of rows of type t
    """
    reducer = build_reducer(reducer)
    if not assume_sorted:
        stream = sort(stream, key)
    pk = None
    for elt in stream:
        if isinstance(elt, StreamHeader):
            if fields or typename:
                metainfo = elt.replace(typename=typename, fields=fields)
            else:
                metainfo = elt
            yield metainfo
        elif isinstance(elt, StreamFooter):
            if pk is not None:
                eg = reducer.end_group(metainfo.t)
                if isinstance(eg, list):
                    for e in eg:
                        yield e
                else:
                    yield eg
            yield elt
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


BabeBase.register('groupBy', group)
BabeBase.register('group', group)


def group_all(stream, reducer, typename=None, fields=None):
    """
    Group all keys
reducer can either be a function or a reducer object
if a function, reducer(t, rows) will be called with all the rows as a parameter
if an object, reducer.begin_group(), reducer.row()
 and reducer.end_group() will be called
    """
    reducer = build_reducer(reducer)
    reducer.begin_group(None)
    for elt in stream:
        if isinstance(elt, StreamHeader):
            if typename or fields:
                metainfo = elt.replace(typename=typename, fields=fields)
            else:
                metainfo = elt
            yield metainfo
        elif isinstance(elt, StreamFooter):
            yield reducer.end_group(metainfo.t)
            yield elt
        else:
            reducer.row(elt)

BabeBase.register('groupAll', group_all)
BabeBase.register('group_all', group_all)