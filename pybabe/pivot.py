
try:
    from collections import OrderedDict
except:
    ## 2.6 Fallback
    from ordereddict import OrderedDict

from base import StreamHeader, StreamFooter, BabeBase


class OrderedDefaultdict(OrderedDict):
    def __init__(self, *args, **kwargs):
        newdefault = None
        newargs = ()
        if args:
            newdefault = args[0]
            if not (newdefault is None or callable(newdefault)):
                raise TypeError('first argument must be callable or None')
            newargs = args[1:]
        self.default_factory = newdefault
        super(self.__class__, self).__init__(*newargs, **kwargs)

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        self[key] = value = self.default_factory()
        return value

    def __reduce__(self):  # optional, for pickle support
        args = self.default_factory if self.default_factory else tuple()
        return type(self), args, None, None, self.items()


class OrderedSet(set):
    def __init__(self):
        self.list = []

    def add(self, elt):
        if elt in self:
            return
        else:
            super(OrderedSet, self).add(elt)
            self.list.append(elt)

    def __iter__(self):
        return self.list.__iter__()


def pivot(stream, pivot, group):
    "Create a pivot around field, grouping on identical value for 'group'"
    groups = OrderedDefaultdict(dict)
    pivot_values = OrderedSet()
    header = None
    group_n = map(StreamHeader.keynormalize, group)
    for row in stream:
        if isinstance(row, StreamHeader):
            header = row
        elif isinstance(row, StreamFooter):
            # HEADER IS : GROUP + (OTHER FIELDS * EACH VALUE
            other_fields = [f for f in header.fields if not f in group and not f == pivot]
            other_fields_k = map(StreamHeader.keynormalize, other_fields)
            fields = group + [f + "-" + str(v)
                for v in pivot_values.list for f in other_fields]
            newheader = header.replace(fields=fields)
            yield newheader
            for _, row_dict in groups.iteritems():
                ## Create a line per group
                mrow = row_dict.itervalues().next()
                group_cols = [getattr(mrow, col) for col in group_n]
                for v in pivot_values:
                    if v in row_dict:
                        mrow = row_dict[v]
                        group_cols.extend([getattr(mrow, col) for col in other_fields_k])
                    else:
                        group_cols.extend([None for col in other_fields])
                yield group_cols
            yield row
        else:
            kgroup = ""
            for f in group_n:
                kgroup = kgroup + str(getattr(row, f))
            groups[kgroup][getattr(row, pivot)] = row
            pivot_values.add(getattr(row, pivot))

BabeBase.register("pivot", pivot)
