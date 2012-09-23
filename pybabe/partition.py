from base import BabeBase, StreamHeader, StreamFooter


def equals_types(t1, t2):
    return t1._fields == t2._fields


def merge_substreams(stream, partition=None):
    header = None
    footer = None
    for row in stream:
        if isinstance(row, StreamHeader):
            if header == None:
                header = row.replace(partition=partition)
                yield header
            else:
                if not equals_types(header.t, row.t):
                    raise Exception('Header types do not match')
        elif isinstance(row, StreamFooter):
            footer = row
        else:
            yield row
    if footer:
        yield footer


BabeBase.register('merge_substreams', merge_substreams)


def partition(stream, field):
    """Create substream per different value of 'column'"""
    beginning = False
    last_value = None
    header = None
    for row in stream:
        if isinstance(row, StreamHeader):
            beginning = True
            header = row
        elif isinstance(row, StreamFooter):
            if beginning == True:
                beginning = False
                continue  # Empty partition: Emit neither header nor footer
            yield row
        else:
            v = getattr(row, field)
            if beginning:
                beginning = False
                last_value = v
                yield header.replace(partition=[(field, v)])
            elif v != last_value:
                yield StreamFooter()
                yield header.replace(partition=[(field, v)])
                last_value = v
            yield row

BabeBase.register('partition', partition)
