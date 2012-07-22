
from base import StreamHeader, StreamFooter, BabeBase
import re

def pull_dump(stream, table, fields, kwargs):
    insert_regex = re.compile("INSERT INTO `%s_\d+` VALUES (.*)\;\r?\n" % table)
    header = StreamHeader(typename=table, fields=fields)
    yield header
    for line in stream:
        m = insert_regex.match(line)
        if m is not None:
            data = match.groups(0)[0]
            for row in data[1:-1].split("),("):
                row.split(',')
                        cb ((pos, '\n'.join([row.replace(',', '^') for row in data[1:-1].split("),(")])))

            for row in data[1:-1].split('),('):
                rr = row.split(',', 1)
                if rr[0] in buf:
                    buf[rr[0]].append(rr[1])
                else:
                    buf[rr[0]] = [rr[1]]
            for k, v in buf.iteritems():
                cb((k, v))


## 140978369311384,674504584,672,'deliv,ered','2011-06-29 13:10:19'