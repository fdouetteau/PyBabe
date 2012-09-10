
from base import BabeBase, StreamHeader, StreamFooter, StreamMeta
import re
      
def mapTo(stream, function, insert_fields = None, fields = None, typename = None):
    """
    Apply a function to a stream. The function receives as input a named tuple. 
    
    if insert_columns is not None, a new stream type is generated with the optional columns 
        an array object with the inserted columns values is expected as a result from the function 
    if columns is not None, a new stream type is generated with only the specified columns
        an array object with the columns values is expected as a result from the function
        
    if neither, a named tuple is expected as a result. namedtuple._make and namedtuple._replace can be used 
    to build the new object.  
    """
    if insert_fields:
        metainfo = None
        for row in stream:
            if isinstance(row, StreamHeader):
                metainfo = row.insert(typename=typename, fields=insert_fields)
                yield metainfo
            elif isinstance(row, StreamMeta):
                yield row 
            else:
                res = function(row)
                if isinstance(res, list) or isinstance(res, tuple):
                    yield metainfo.t._make(row + tuple(res))
                else:
                    yield metainfo.t._make(row + (res,))
    elif fields:
        metainfo = None
        for row in stream:
            if isinstance(row, StreamHeader):
                metainfo = row.replace(typename=typename, fields=fields)
                yield metainfo
            elif isinstance(row, StreamMeta):
                yield row
            else:
                yield metainfo.t._make(function(row))
    elif typename:
        metainfo = None
        for row in stream:
            if isinstance(row, StreamHeader):
                metainfo = row.augment(typename=typename, fields=[])
                yield metainfo
            elif isinstance(row, StreamMeta):
                yield row 
            else:
                yield metainfo.t._make(list(function(row)))
    else:
        for row in stream:
            if isinstance(row, StreamMeta):
                yield row
            else: 
                yield function(row)
    
BabeBase.register("mapTo", mapTo)

def bulkMapTo(stream, function, bulk_size, insert_fields = None, fields = None): 
    header = None
    buf = []
    for row in stream: 
        if isinstance(row, StreamHeader): 
            if insert_fields: 
                header = row.insert(typename=None, fields=insert_fields)
            elif fields: 
                header = row.insert(typename=None, fields=fields)
            else:
                header = row
            yield header
        elif isinstance(row, StreamFooter) or len(buf) == bulk_size - 1:
            if not isinstance(row, StreamFooter): 
                buf.append(row)
            result =  function(buf)
            if insert_fields:
                for i, r in enumerate(result):
                    yield header.t._make((buf[i] + tuple(r)))
            else:
                for r in result:
                    yield header.t._make(r)
            del buf[:]
            if isinstance(row, StreamFooter):
                yield row
        else: 
            buf.append(row)


BabeBase.register("bulkMapTo", bulkMapTo)


def replace_in_string(stream, match, replacement, field):
    for row in stream:
        if isinstance(row, StreamMeta):
            yield row
        else:
            v = getattr(row, field)
            if v is None:
                yield row
            else:
                yield row._replace(**{field:v.replace(match, replacement)})

BabeBase.register("replace_in_string", replace_in_string)

def flatMap(stream, function, insert_columns = None, columns = None, name = None):
    if insert_columns:
        metainfo = None
        for row in stream:
            if isinstance(row, StreamHeader):
                metainfo = row.insert(name=name, names=insert_columns)
                yield metainfo
            elif isinstance(row, StreamMeta):
                yield row 
            else:
                res = function(row)
                for r in res:
                    yield metainfo.t._make(list(row) + r)
    elif columns:
        metainfo = None
        for row in stream:
            if isinstance(row, StreamHeader):
                metainfo = row.replace(name=name, names=columns)
                yield metainfo
            elif isinstance(row, StreamMeta):
                yield row 
            else:
                for r in function(row):
                    yield metainfo.t._make(list(r))
    elif name:
        metainfo = None
        for row in stream:
            if isinstance(row, StreamHeader):
                metainfo = row.augment(name=name, names=[])
                yield metainfo
            elif isinstance(row, StreamMeta):
                yield row 
            else:
                for r in function(row):
                    yield metainfo.t._make(list(r))
    else:
        for row in stream:
            if isinstance(row, StreamMeta):
                yield row
            else: 
                for r in function(row):
                    yield r

BabeBase.register("flatMap", flatMap)

def skip(stream, n):
    """Skip the first n row""" 
    for row in stream: 
        if isinstance(row, StreamHeader): 
            count = 0 
            yield row 
        elif isinstance(row, StreamFooter): 
            yield row
        else: 
            if count >= n:
                yield row
            count = count + 1

BabeBase.register('skip', skip)
      
def head(stream, n, all_streams = False):
    """Retrieve only the first n lines. 
    If all_streams is true, apply head on each substream
    Otherwise (default), only keep the first substream."""
    if not all_streams:
        for row in stream: 
            if isinstance(row, StreamHeader):
                count = 0 
            elif isinstance(row, StreamFooter):
                break
            else: 
                if count >= n: 
                    yield StreamFooter()
                    break
                count = count + 1
            yield row
    else:
        skip = False 
        for row in stream:
            if isinstance(row, StreamHeader):
                count = 0
            elif isinstance(row, StreamFooter):
                skip = False
            else:
                if count >= n: 
                    skip = True 
                count = count + 1 
            if not skip:
                yield row 


BabeBase.register('head', head)

def split(stream, field, separator):
    for row in stream:
        if isinstance(row, StreamMeta):
            yield row
        else:
            value = getattr(row, field)
            values = value.split(separator)
            for v in values:
                yield row._replace(**{field:v})
BabeBase.register('split',split)

def replace(stream, oldvalue, newvalue, column = None):
    buf = []
    for row in stream:
        if isinstance(row, StreamMeta):
            yield row
        else:
            del buf[:] 
            change = False 
            for v in row:
                if v == oldvalue: 
                    buf.append(newvalue)
                    change = True
                else:
                    buf.append(v)
            if change:
                yield row._make(buf)
            else: 
                yield row 
                
BabeBase.register('replace', replace)

def filterColumns(stream, typename=None, remove_fields=None, keep_fields=None):
    for row in stream:
        if isinstance(row, StreamHeader):
            if keep_fields:
                fields= keep_fields
            else:
                fields = [name for name in row.normalized_fields if not name in remove_fields] 
            metainfo = row.replace(typename=typename, fields=fields)
            yield metainfo
        elif isinstance(row, StreamMeta):
            yield row
        else:
            yield metainfo.t._make([getattr(row, k) for k in fields])

BabeBase.register('filterColumns', filterColumns)

def filter_values(stream, **kwargs):
    for row in stream:
        if isinstance(row, StreamMeta):
            yield row
        else:
            ok = True
            for k in kwargs:
                if not kwargs[k] == getattr(row, k):
                    ok = False
                    break 
            if ok:
                yield row

BabeBase.register('filter_values', filter_values)


def filter_out_null_values(stream, fields): 
    for row in stream:
        if isinstance(row, StreamMeta):
            yield row
        else: 
            keep = True 
            for f in fields: 
                if getattr(row, f) == None:
                    keep = False 
                    break 
            if keep: 
                yield row 

BabeBase.register("filter_out_null_values", filter_out_null_values)

def filter(stream, function):
    for row in stream:
        if isinstance(row, StreamMeta):
            yield row
        else:
            if function(row):
                yield row

BabeBase.register('filter', filter)


def filter_by_regexp(stream, field, regexp): 
    pat = re.compile(regexp)
    for row in stream: 
        if isinstance(row, StreamMeta): 
            yield row
        else: 
            v = getattr(row, field)
            if v and pat.match(v):
                yield row 

BabeBase.register("filter_by_regexp", filter_by_regexp)

def rename(stream, **kwargs):
    for row in stream:
        if isinstance(row, StreamHeader):
            metainfo = row.replace(typename=None, fields=[kwargs.get(name, name) for name in row.fields])
            yield metainfo
        elif isinstance(row, StreamMeta):
            yield row 
        else:
            yield metainfo.t._make(list(row))
        
BabeBase.register('rename', rename)

class Window(object):
    def __init__(self, size):
        self.size = size
        self.buf = []
    def add(self, obj):
        if len(self.buf) == self.size:
            self.buf.pop(0)
        self.buf.append(obj)
        
def windowMap(stream, window_size, function, insert_fields = None, fields = None, typename = None):
    """
Similar to mapTo. 
For each row, function(rows) is called with the last 'window_size' rows
    """
    window = Window(window_size)
    if insert_fields:
          metainfo = None
          for row in stream:
                if isinstance(row, StreamHeader):
                    metainfo = row.insert(typename=typename, fields=insert_fields)
                    yield metainfo
                elif isinstance(row, StreamMeta):
                    yield row 
                else:
                    window.add(row)
                    res = function(window.buf)
                    if isinstance(res, list):
                        yield metainfo.t._make(list(row) + res)
                    else:
                        yield metainfo.t._make(list(row) + [res])
    elif fields:
          metainfo = None
          for row in stream:
                if isinstance(row, StreamHeader):
                    metainfo = row.replace(typename=typename, fields=fields)
                    yield metainfo
                elif isinstance(row, StreamMeta):
                    yield row
                else:
                    window.add(row)
                    yield metainfo.t._make(function(window.buf))
    elif typename:
          metainfo = None
          for row in stream:
                if isinstance(row, StreamHeader):
                    metainfo = row.augment(typename=typename, fields=[])
                    yield metainfo
                elif isinstance(row, StreamMeta):
                    yield row
                else:
                    window.add(row)
                    yield metainfo.t._make(list(function(window.buf)))
    else:
          for row in stream:
                if isinstance(row, StreamMeta):
                    yield row
                else: 
                    window.add(row)
                    yield function(window.buf)

BabeBase.register('windowMap', windowMap)

def transpose(stream, typename=None):
    """
    Transpose a stream. 
    For each row, the 'unique identifier' for this row will be used as a column name. 
    city, b, c
    PARIS, foo, bas
    LONDON, coucou, salut

    field, PARIS,LONDON
    city, PARIS, LONDON
    b, foo, coucou
    c, bas, salut

    b,c
    foo,bar
    coucou,salut

    field, 1, 2 
    b,foo, coucou
    c,bar,salut

    """
    for row in stream:
        if isinstance(row, StreamHeader):
            metainfo = row
            linecount = 0 
            t_names = ['field']
            t_primary_key = 'field'
            t_rows = [[name] for name in metainfo.fields]
        elif isinstance(row, StreamFooter):
            t_metainfo= StreamHeader(source=metainfo.source, typename=typename, fields=t_names, primary_key=t_primary_key)
            yield t_metainfo
            for t_row in t_rows: 
                if t_row[0] == metainfo.primary_key: # Skip primary key 
                    continue
                yield t_metainfo.t(*t_row) 
            yield row 
        else:
            linecount = linecount + 1
            c_id = metainfo.get_primary_identifier(row, linecount)
            t_names.append(c_id)    
            for i, cell in enumerate(row): 
                t_rows[i].append(cell)

BabeBase.register('transpose', transpose)

def build_row(header, row):
    return header.t(*row)

def insert_rows(stream, new_rows, before=True):
    for row in stream:
        if isinstance(row, StreamHeader):
            yield row
            if before: 
                for r in new_rows: 
                    yield build_row(row, r)
        elif isinstance(row, StreamFooter):
            if not before:
                for r in new_rows:
                    yield build_row(row,r)
            yield row 
        else:
            yield row

BabeBase.register('insert_rows', insert_rows)

