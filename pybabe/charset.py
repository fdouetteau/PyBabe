

import codecs
import csv
import cStringIO
import datetime

## From samples in http://docs.python.org/library/csv.html

class UTF8Recoder(object):
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    Made mandatory by the csv module operating only on 'str'  
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")
        
class UTF8RecoderWithCleanup(UTF8Recoder): 
    "Rencode a stream in utf-8, with 'charset' clenaup algorithm in the middle"
    def __init__(self, f, encoding):
        super(UTF8RecoderWithCleanup, self).__init__(f, encoding)
        from encoding_cleaner import get_map_table
        (regex, m) = get_map_table(encoding, 'latin1')
        self.regex = regex
        self.m = m  
    def next(self):
        u = self.reader.next()
        tu = self.regex.sub(lambda g: self.m[g.group(0)], u)
        return tu.encode('utf-8')
        
class PrefixReader(object):
    def __init__(self, prefix, stream, linefilter):
        self.prefix = prefix
        self.stream = stream
        self.linefilter = linefilter
    def __iter__(self):
        yield self.prefix
        linefilter = self.linefilter
        if linefilter:
            for k in self.stream:
                if linefilter(k):
                    yield k
        else:
            for k in self.stream:
                yield k 

def write_value(s):
    if isinstance(s, unicode):
        return s.encode('utf-8')
    elif isinstance(s, datetime.datetime):
        return s.strftime('%Y-%m-%d %H:%M:%S') # Remove timezone
    else:
        return s
            
class UnicodeCSVWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        if encoding == 'utf_16_le': 
            self.stream.write(codecs.BOM_UTF16_LE)
        elif encoding == 'utf_16_be': 
            self.stream.write(codecs.BOM_UTF16_BE)
        elif encoding == 'utf_16': 
            self.stream.write(codecs.BOM_UTF16)
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow(map(write_value, row))
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)