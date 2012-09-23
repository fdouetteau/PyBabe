
from base import BabeBase, StreamFooter
import datetime


def write_value(v, encoding):
    if isinstance(v, unicode):
        return v.encode(encoding)
    elif isinstance(v, datetime.datetime):
        # Remove timezonel
        return v.strftime('%Y-%m-%d %H:%M:%S')
    elif v is None:
        return ""
    else:
        return v


def write(format, header, instream, outfile, encoding, **kwargs):
    if not encoding:
        encoding = "utf-8"
    outfile.write("<h2>")
    outfile.write(header.get_stream_name())
    outfile.write("</h2>")
    if header.description:
        outfile.write("<p><i>")
        outfile.write(header.description)
        outfile.write("</i></p>")
    outfile.write('<table>\n<tr>')
    for field in header.fields:
        outfile.write("<th>")
        outfile.write(write_value(field, encoding))
        outfile.write("</th>")
    outfile.write("</tr>\n")
    for row in instream:
        if isinstance(row, StreamFooter):
            outfile.write("</table>\n")
            break
        else:
            outfile.write("<tr>")
            for cell in row:
                outfile.write("<td>")
                outfile.write(write_value(cell, encoding))
                outfile.write("</td>")
            outfile.write("</tr>\n")


BabeBase.addPushPlugin('html', ['html', 'htm'], write)
