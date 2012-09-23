
from base import BabeBase, StreamHeader, StreamMeta


def dedup(stream, fields=None):
    """
Deduplicate a stream
If columns is specified only apply the  deduplication on the specified columns
Otherwise apply the deduplication over all values.
    """
    for row in stream:
        if isinstance(row, StreamHeader):
            metainfo = row
            if fields:
                indexes = [metainfo.fields.index(c) for c in fields]
            else:
                indexes = None
            s = set()
            yield row
        elif isinstance(row, StreamMeta):
            yield row
        else:
            if indexes:
                l = list(row)
                v = tuple([l[i] for i in indexes])
            else:
                v = row
            if v in s:
                pass
            else:
                yield row
                s.add(v)


BabeBase.register('dedup', dedup)
