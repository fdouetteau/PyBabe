
import csv 
from collections import namedtuple
import itertools
import re
from timeparse import parse_date, parse_datetime

class Babe(object):
    def pull(self, resource, name, format=None, **kwargs):
        ## Open File
        stream = None
        if hasattr(resource,'read'): 
            stream = resource
        elif isinstance(resource, str): 
            if resource.endswith('.xlsx'):
                from openpyxl import load_workbook
                wb = load_workbook(filename =resource, use_iterators = True)
                ws = wb.get_active_sheet() # ws is now an IterableWorksheet
                return ExcelPull(name, ws) 
            else:
                stream = open(resource, 'rb')
        sniff_read = stream.readline()
        stream.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sniff_read)
        except:
            raise Exception ()
        return CSVPull(name, stream, dialect)
  
    def map(self, column,  f):
        return Map(f, column, self)
        
    def multimap(self, d):
        return MultiMap(self, d)
        
    def typedetect(self):
        "Create a stream where integer/floats are automatically detected"
        return TypeDetect(self)
        
    def sort(self, key):
        "Return a sorted input according to key"
        return Sort(self, key)
        
    def groupkey(self, key, red, initial_value, group_key=None, keepOriginal=False):
        """Group all elements with equal value for group_key. 
        value = red(value, row[key]) is called for each row with equal value for group_key
        See 'group'  
        """
        kr = KeyReducer()
        kr.key = key
        kr.reduce = red
        kr.initial_value = initial_value
        return self.group(kr, group_key=group_key, keepOriginal=keepOriginal)

    def keynormalize(self, key):
        """Normalize a column name to a valid python identifier"""
        return '_'.join(re.findall(r'\w+',key))


    def group(self, reducer, group_key = None, keepOriginal=False):
        """Group all elements with equal value for key, assuming sorted input.
        reducer.begin_group() is called each time a new value for key 'key' is found
        reducer.row(row) is called on each row
        reducer.group_result() is called after the last row containing a equal value for that key.
        It shall return a new value to emit.  
        If keepOriginal is True, original lines will be kept in the output streamalongside grouped values.
        If key is None all keys are group together. new_group() and end_group() are called once."""
        if group_key is None:
            return GroupAll(self, reducer, keepOriginal)
        else:
            return Group(self, group_key, reducer, keepOriginal)
            
    def push(self, resource, format=None, **kwards):
        metainfo = None
        writer = None
        outstream = None
        if hasattr(resource, 'write'): 
            outstream = resource
        elif isinstance(resource, str) and resource.endswith('.xlsx'):
            from openpyxl import Workbook
            wb = Workbook(optimized_write = True)
            ws = wb.create_sheet()
            for k in self:
                if isinstance(k, MetaInfo):
                    metainfo = k
                    ws.append(metainfo.names)
                else:
                    ws.append(list(k))
            wb.save(resource)
        elif isinstance(resource, str):
            outstream = open(resource, 'wb')
        else:
            raise Exception()
        
        if outstream:
            for k in self: 
                if isinstance(k, MetaInfo):
                    metainfo = k
                    writer = csv.writer(outstream, metainfo.dialect)
                    writer.writerow(metainfo.names)
                else:
                    writer.writerow(list(k))
                
class Sort(Babe):
    def __init__(self, stream, key):
        self.stream = stream
        self.key = key
        self.buffer = []
    def __iter__(self):
        count = 0
        for elt in self.stream:
            if isinstance(elt, MetaInfo):
                yield elt
            else:
                self.buffer.append((getattr(elt, self.key), count, elt))
                count = count + 1
        self.buffer.sort()
        for (k, c, elt) in self.buffer:
            yield elt

class GroupAll(Babe):
    def __init__(self, stream, reducer, keepOriginal):
        self.stream = stream
        self.reducer = reducer
        self.keepOriginal = keepOriginal
    def __iter__(self):
        self.reducer.begin_group()
        for elt in self.stream:
            if isinstance(elt, MetaInfo):
                yield elt
            else:
                if self.keepOriginal:
                    yield elt
                self.reducer.row(elt)
        yield self.reducer.group_result()
            
class Group(Babe):
    def __init__(self, stream, key, reducer, keepOriginal):
        self.stream = stream
        self.key = key
        self.reducer = reducer
        self.keepOriginal = keepOriginal
    def __iter__(self):
        pk = None
        for elt in self.stream:
            if isinstance(elt, MetaInfo):
                yield elt
            else:
                if self.keepOriginal:
                    yield elt 
                k = getattr(elt, self.key)
                if (pk is not None) and not (pk == k):
                    yield self.reducer.group_result()
                    self.reducer.begin_group()
                    pk = k 
                    self.reducer.row(elt)
                else:
                    self.reducer.row(elt)
        if pk is not None:
            yield self.reducer.group_result()    
                
                    
class Map(Babe):
    def __init__(self, f, column, stream):
        self.f = f
        self.column = column 
        self.stream = stream 
    def __iter__(self):
        return itertools.imap(lambda elt : elt._replace(**{self.column : self.f(getattr(elt, self.column))}) if not isinstance(elt, MetaInfo) else elt,
               self.stream)
               

class MultiMap(Babe):
    def __init__(self, stream, d):
        self.stream = stream 
        self.d = d
    def map(self, elt):
        if isinstance(elt, MetaInfo):
            return elt
        m = {}
        for k in self.d:
            m[k] = self.d[k](getattr(elt, k))
        return elt._replace(**m) 
    def __iter__(self):
        return itertools.imap(self.map, self.stream)
               
class TypeDetect(Babe):
    
    patterns = [r'(?P<int>[0-9]+)', 
         r'(?P<float>[0-9]+\.[0-9]+)',
         r'(?P<date>\d{2,4}/\d\d/\d\d|\d\d/\d\d/\d\d{2,4})', 
         r'(?P<datetime>\d\d/\d\d/\d\d{2,4} \d{2}:\d{2})'
        ]
         
    
    pattern = re.compile('(' + '|'.join(patterns) + ')$')
    
    def __init__(self, stream):
        self.stream = stream
        self.d = {}
    def __iter__(self):
        return itertools.imap(self.filter, self.stream)
    def filter(self, elt):
        if isinstance(elt, MetaInfo):
            return elt
        else:
            self.d.clear()
            for t in elt._fields:
                v = getattr(elt, t)
                if not isinstance(v, str):
                    continue
                g = self.pattern.match(v)
                if g: 
                    if g.group('int'):
                        self.d[t] = int(v)
                    elif g.group('float'):
                        self.d[t] = float(v)
                    elif g.group('date'):
                        self.d[t] = parse_date(v)
                    elif g.group('datetime'):
                        self.d[t] = parse_datetime(v)
            if len(self.d) > 0:
                return elt._replace(**self.d)
            else:
                return elt
        
class CSVPull(Babe):
    def __init__(self, name, stream, dialect):
        self.name = name
        self.stream = stream
        self.dialect = dialect
    def __iter__(self):
        r = csv.reader(self.stream, self.dialect)
        names  = r.next()
        names = [s.strip() for s in names] # Normalize names
        metainfo = MetaInfo()
        metainfo.dialect = self.dialect
        metainfo.names = names
        t = namedtuple(self.name, map(self.keynormalize,names))
        yield metainfo
        for row in r:
            yield t._make(row)
            
class ExcelPull(Babe):
    def __init__(self, name, ws):
        self.name = name
        self.ws = ws
    def __iter__(self):
        it = self.ws.iter_rows()
        names_row = it.next()
        names = [str(cell.internal_value).strip() for cell in names_row]
        metainfo = MetaInfo()
        metainfo.names = names
        t = namedtuple(self.name, map(self.keynormalize,names))
        yield  metainfo
        for row in it: # it brings a new method: iter_rows()
            yield t._make([cell.internal_value for cell in row])
        
class MetaInfo(object): 
    dialect = None

class KeyReducer(object):
    def begin_group(self):
        self.value = self.initial_value
    def row(self, row):
        self.last_row = row
        self.value = self.reduce(self.value, getattr(row, self.key))
    def group_result(self):
        return self.last_row._replace(**{self.key: self.value})
