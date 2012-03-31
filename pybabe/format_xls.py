

from base import MetaInfo, BabeBase

def valuenormalize(cell):
    return cell.value 

def read(format, instream, name, names, encoding, utf8_cleanup, **kwargs):
    import xlrd
    wb = xlrd.open_workbook(file_contents=instream.read(), encoding_override=encoding)
    ws = wb.sheet_by_index(0)
    nrows = ws.nrows
    if names: 
        yield MetaInfo(name=name, names = names)
    	b = 0 
    else:
    	b = 1
        names_row = ws.row(0)
        names = [cell.value for cell in names_row]
        metainfo =  MetaInfo(name=name, names=names)
        yield metainfo
    	for i in xrange(b, nrows):
            cells = ws.row(i)
            yield metainfo.t._make(map(valuenormalize, cells))

BabeBase.addPullPlugin('xls', ['xls'], read, need_seek=False)
