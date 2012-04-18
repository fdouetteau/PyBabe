
from base import StreamHeader, BabeBase, StreamFooter

def valuenormalize(cell):
    "Build the row value out of a cell"
    if cell.number_format == '0': 
        return int(cell.internal_value)
    else: 
        return cell.internal_value

def read(format, stream, name, names, kwargs):
    from openpyxl import load_workbook
    wb = load_workbook(filename=stream, use_iterators=True)
    ws = wb.get_active_sheet()
    it = ws.iter_rows()
    if names: 
        yield StreamHeader(name=name, names = names)
    else:
        names_row = it.next()
        names = [cell.internal_value for cell in names_row]
        metainfo =  StreamHeader(name=name, names=names)
        yield metainfo
    for row in it: # it brings a new method: iter_rows()
        yield metainfo.t._make(map(valuenormalize, row))
    yield StreamFooter()

def write(format, metainfo, instream, outfile, encoding, **kwargs):
    from openpyxl import Workbook
    wb = Workbook(optimized_write = True)
    ws = wb.create_sheet()
    ws.append(metainfo.names)
    for k in instream:
        if isinstance(k, StreamFooter):
            break
        else:
            ws.append(list(k))
    wb.save(outfile)

BabeBase.addPullPlugin('xlsx', ['xlsx'], read, need_seek=True)
BabeBase.addPushPlugin('xlsx', ['xlsx'], write)

