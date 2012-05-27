
from base import BabeBase, StreamFooter
import datetime

def write_value(v, encoding): 
    if isinstance(v, unicode): 
        return v.encode(encoding)
    elif isinstance(v, datetime.datetime):
        return v.strftime('%Y-%m-%d %H:%M:%S') # Remove timezone
    elif v is None: 
        return ""
    else: 
        return v

def write(format, header, instream, outfile, encoding, **kwargs):
    if not encoding: 
        encoding = "utf-8"
    outfile.write('<table><tr>')
    for field in header.fields:
        outfile.write("<th>")
        outfile.write(write_value(field, encoding))
        outfile.write("</th>")
    outfile.write("</tr>")
    for row in instream: 
        if isinstance(row, StreamFooter): 
            outfile.write("</table>")
            break
        else: 
            outfile.write("<tr>")
            for cell in row: 
                outfile.write("<td>")
                outfile.write(write_value(cell, encoding))
                outfile.write("</td>")
            outfile.write("</tr>")
            
BabeBase.addPushPlugin('html', ['html', 'htm'], write)   