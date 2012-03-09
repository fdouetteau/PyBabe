
import re, itertools
from base import MetaInfo, BabeBase
from timeparse import parse_date, parse_datetime

patterns = [r'(?P<int>-?[0-9]+)', 
     r'(?P<float>-?[0-9]+\.[0-9]+)',
     r'(?P<date>\d{2,4}/\d\d/\d\d|\d\d/\d\d/\d\d{2,4})', 
     r'(?P<datetime>\d\d/\d\d/\d\d{2,4} \d{2}:\d{2})'
    ]
         
pattern = re.compile('(' + '|'.join(patterns) + ')$')

def typedetect(stream):
    return itertools.imap(typefilter, stream)

def typefilter(elt):
    if isinstance(elt, MetaInfo):
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