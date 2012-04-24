

from base import StreamHeader, BabeBase, StreamFooter

def valuenormalize(cell):
    return cell.value 

def read(format, stream, kwargs):
    import xlrd
    wb = xlrd.open_workbook(file_contents=stream.read(), encoding_override=kwargs.get('encoding', None))
    ws = wb.sheet_by_index(0)
    nrows = ws.nrows
    fields = kwargs.get('fields', None)
    if not fields: 
        b = 1 
        fields = [cell.value for cell in ws.row(0)] 
    else: 
        b = 0 
    metainfo = StreamHeader(**dict(kwargs, fields=fields))
    yield metainfo
    for i in xrange(b, nrows):
        cells = ws.row(i)
        yield metainfo.t._make(map(valuenormalize, cells))
    yield StreamFooter()

BabeBase.addPullPlugin('xls', ['xls'], read, need_seek=False)
