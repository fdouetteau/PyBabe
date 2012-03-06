

import codecs

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
    def __init__(self, prefix, stream):
        self.prefix = prefix
        self.stream = stream
    def __iter__(self):
        yield self.prefix
        for k in self.stream:
            yield k 
