

from base import StreamHeader, BabeBase, StreamFooter

def valuenormalize(cell):
    return cell.value 

def read(format, stream, name, names, kwargs):
    import xlrd
    wb = xlrd.open_workbook(file_contents=stream.read(), encoding_override=kwargs.get('encoding', None))
    ws = wb.sheet_by_index(0)
    nrows = ws.nrows
    if names: 
        yield StreamHeader(name=name, names = names)
    	b = 0 
    else:
    	b = 1
        names_row = ws.row(0)
        names = [cell.value for cell in names_row]
        metainfo =  StreamHeader(name=name, names=names)
        yield metainfo
    	for i in xrange(b, nrows):
            cells = ws.row(i)
            yield metainfo.t._make(map(valuenormalize, cells))
    yield StreamFooter()

BabeBase.addPullPlugin('xls', ['xls'], read, need_seek=False)
