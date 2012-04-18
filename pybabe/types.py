
import re, itertools
from base import StreamHeader, BabeBase
from timeparse import parse_date, parse_datetime
from collections import deque

patterns = [r'(?P<int>-?[0-9]+)', 
     r'(?P<float>-?[0-9]+\.[0-9]+)',
     r'(?P<date>\d{2,4}/\d\d/\d\d|\d\d/\d\d/\d\d{2,4})', 
     r'(?P<datetime>\d\d/\d\d/\d\d{2,4} \d{2}:\d{2})'
    ]
         
pattern = re.compile('(' + '|'.join(patterns) + ')$')

def typedetect(stream):
    return itertools.imap(typefilter, stream)

def typefilter(elt):
    if isinstance(elt, StreamHeader):
        return elt
    else:
        d = {}
        for t in elt._fields:
            v = getattr(elt, t)
            if not isinstance(v, basestring):
                continue
            g = pattern.match(v)
            if g: 
                if g.group('int'):
                    d[t] = int(v)
                elif g.group('float'):
                    d[t] = float(v)
                elif g.group('date'):
                    d[t] = parse_date(v)
                elif g.group('datetime'):
                    d[t] = parse_datetime(v)
        if len(d) > 0:
            return elt._replace(**d)
        else:
            return elt
BabeBase.register("typedetect", typedetect)


def primary_key_detect(stream, max=None): 
    d = deque()
    it = iter(stream)
    for linecount, row in enumerate(it):
        d.append(row)
        if isinstance(row,StreamHeader): 
            metainfo = row
            values = [set() for k in metainfo.names]
            keys = set(xrange(0,len(metainfo.names)))
        else:
            for idx, val in enumerate(row):
                if values[idx] is not None:
                    if val in values[idx]:
                        values[idx] = None
                        # print "Duplicate column %u on line %u" % (idx, linecount)
                        keys.remove(idx)
                    else:
                        values[idx].add(val)
            if len(keys) <= 1:
                break
    if len(keys) == 1:
        metainfo.primary_keys = [metainfo.names[list(keys)[0]]]
        #print "Detected primary key %s" % str(metainfo.primary_keys)
    elif len(keys) == 0:
        pass # print 'no primary key'
    else:
        metainfo.primary_keys = [metainfo.names[min(keys)]]
        #  print "Assumed primary key %s" % str(metainfo.primary_keys)
    for row in d:
        yield row
    for row in it:
        yield row

BabeBase.register('primary_key_detect', primary_key_detect)


             

