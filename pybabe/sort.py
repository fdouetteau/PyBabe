
from base import StreamHeader, BabeBase, StreamFooter
from tempfile import TemporaryFile
import cPickle
import heapq
import itertools


def sort(stream, field, reverse=False):
    buf = []
    for elt in stream:
        if isinstance(elt, StreamHeader):
            yield elt
        elif isinstance(elt, StreamFooter):
            buf.sort(key=lambda obj: getattr(obj, field), reverse=reverse)
            for row in buf:
                yield row
            yield elt
        else:
            buf.append(elt)

BabeBase.register('sort', sort)


def sort_diskbased(stream, field, nsize=100000):
    buf = []
    files = []
    count = 0
    t = None

    def iter_on_file(f):
        try:
            while True:
                (key, v) = cPickle.load(f)
                yield (key, t._make(v))
        except EOFError:
            f.close()
    for elt in stream:
        if isinstance(elt, StreamHeader):
            t = elt.t
            yield elt
        elif isinstance(elt, StreamFooter):
            buf.sort()
            iterables = [iter_on_file(f) for f in files] + [itertools.imap(lambda obj: (getattr(obj, field), obj), buf)]
            for (k, row) in heapq.merge(*iterables):
                yield row
            yield elt
        else:
            buf.append(elt)
            count = count + 1
            if count % nsize == 0:
                buf.sort(key=lambda obj: getattr(obj, field))
                f = TemporaryFile()
                for item in buf:
                    cPickle.dump((getattr(item, field), list(item)), f, cPickle.HIGHEST_PROTOCOL)
                f.flush()
                files.append(f)
                del buf[:]

BabeBase.register('sort_diskbased', sort_diskbased)
