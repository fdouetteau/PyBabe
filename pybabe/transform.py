
from base import BabeBase, MetaInfo
      
def mapTo(stream, function, insert_columns = None, replace_columns = None, name = None):
    """
    Apply a function to a stream. The function receives as input a named tuple. 
    
    if insert_columns is not None, a new stream type is generated with the optional columns 
        an array object with the inserted columns values is expected as a result from the function 
    if replace_colunns is not None, a new stream type is generated with only the specified columns
        an array object with the columns values is expected as a result from the function
        
    if neither, a named tuple is expected as a result. namedtuple._make and namedtuple._replace can be used 
    to build the new object.  
    """
    if insert_columns:
        metainfo = None
        for row in stream:
            if isinstance(row, MetaInfo):
                metainfo = row.insert(name=name, names=insert_columns)
                yield metainfo
            else:
                res = function(row)
                if isinstance(res, list):
                    yield metainfo.t._make(list(row) + res)
                else:
                    yield metainfo.t._make(list(row) + [res])
    elif replace_columns:
        metainfo = None
        for row in stream:
            if isinstance(row, MetaInfo):
                metainfo = row.replace(name=name, names=replace_columns)
                yield metainfo
            else:
                yield metainfo.t._make(function(row))
    elif name:
        metainfo = None
        for row in stream:
            if isinstance(row, MetaInfo):
                metainfo = row.augment(name=name, names=[])
                yield metainfo
            else:
                yield metainfo.t._make(list(function(row)))
    else:
        for row in stream:
            if isinstance(row, MetaInfo):
                yield row
            else: 
                yield function(row)
    
BabeBase.register("mapTo", mapTo)

def flatMap(stream, function, insert_columns = None, replace_columns = None, name = None):
    if insert_columns:
        metainfo = None
        for row in stream:
            if isinstance(row, MetaInfo):
                metainfo = row.insert(name=name, names=insert_columns)
                yield metainfo
            else:
                res = function(row)
                for r in res:
                    yield metainfo.t._make(list(row) + r)
    elif replace_columns:
        metainfo = None
        for row in stream:
            if isinstance(row, MetaInfo):
                metainfo = row.replace(name=name, names=replace_columns)
                yield metainfo
            else:
                for r in function(row):
                    yield metainfo.t._make(list(r))
    elif name:
        metainfo = None
        for row in stream:
            if isinstance(row, MetaInfo):
                metainfo = row.augment(name=name, names=[])
                yield metainfo
            else:
                for r in function(row):
                    yield metainfo.t._make(list(r))
    else:
        for row in stream:
            if isinstance(row, MetaInfo):
                yield row
            else: 
                for r in function(row):
                    yield r

BabeBase.register("flatMap", flatMap)

      
def head(stream, n):
    """Retrieve only the first n line of the stream"""
    for row in stream: 
        if isinstance(row, MetaInfo):
            count = 0 
        else: 
            if count >= n: 
                break
            count = count + 1
        yield row
BabeBase.register('head', head)

def split(stream, column, separator):
    for row in stream:
        if isinstance(row, MetaInfo):
            yield row
        else:
            value = getattr(row, column)
            values = value.split(separator)
            for v in values:
                yield row._replace(**{column:v})
BabeBase.register('split',split)

def replace(stream, oldvalue, newvalue, column = None):
    buf = []
    for row in stream:
        if isinstance(row, MetaInfo):
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

def filterColumns(stream, name=None, remove_columns=None, keep_columns=None):
    for row in stream:
        if isinstance(row, MetaInfo):
            if keep_columns:
                names = keep_columns
            else:
                names = [name for name in row.names if not name in remove_columns] 
            metainfo = row.replace(name=name, names=names)
            yield metainfo
        else:
            yield metainfo.t._make([getattr(row, k) for k in names])

BabeBase.register('filterColumns', filterColumns)

def filter(stream, function):
    for row in stream:
        if isinstance(row, MetaInfo):
            yield row
        else:
            if function(row):
                yield row

BabeBase.register('filter', filter)

def rename(stream, **kwargs):
    for row in stream:
        if isinstance(row, MetaInfo):
            metainfo = row.replace(name=None, names=[kwargs.get(name, name) for name in row.names])
            yield metainfo
        else:
            yield metainfo.t._make(list(row))
        
BabeBase.register('rename', rename)