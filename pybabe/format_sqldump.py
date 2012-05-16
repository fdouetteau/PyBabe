
from base import BabeBase, StreamFooter, StreamHeader
import sys
import re 

pattern = r"'((?:(?:\\.)|[^'\\])*)'|((?:\d|\.)+)|(NULL)" 
pat = re.compile(pattern)


def parse_value(pos, line):
	m = pat.match(line, pos)
	if m: 
		if m.lastindex == 3:
			return (None, m.end(0)) 
		elif m.lastindex ==2:
			return (m.group(2), m.end(0))
		else: 
			return (unescape(m.group(1)), m.end(0))
	else:
		raise Exception("ParseError %s", line[pos:pos + 10 if pos+10 < len(line)-1 else len(line)-1 ])

def unescape(s): 
	if s.find('\\') >= 0: 
		s = s.replace("\\'", "'")
		s = s.replace("\\n", "\n")
		s = s.replace("\\r", "\r")
	return s


def parse_tuple(pos, line): 
	if line[pos] != '(':
		raise Exception("ParseError")
	pos = pos+1
	buf = []
	while True :
		(val, pos) = parse_value(pos, line)
		buf.append(val)
		if line[pos] == ',': 
			pos = pos+1
		elif line[pos] == ')':
			pos = pos+1
			break
		else: 
			raise Exception("ParseError %s", line[pos:pos + 10 if pos+10 < len(line)-1 else len(line)-1 ]) 
	return (buf, pos) 

def pull(format, stream, kwargs): 
	"""
	Read a SQL dump "INSERT VALUE" statements from a single table 

	table = The name of the table to read (mandatory)
	fields = The sets 
	"""

	fields = kwargs['fields']
	table = kwargs['table']
	header = StreamHeader(fields=fields, table=table)
	yield header 
	prefix = "INSERT INTO `%s` VALUES " % table 
	for line in stream: 
		if not line.startswith(prefix):
			continue
		pos = len(prefix)
		while pos < len(line):
			(elts, pos) = parse_tuple(pos, line)
			yield header.t(*elts)
			if line[pos] == ',':
				pos = pos+1
				continue
			elif line[pos] == ';':
				break
			else:
				raise Exception("ParseError pos %u " % pos)
	yield StreamFooter()

BabeBase.addPullPlugin("sql", ["sql"], pull)

if __name__ == "__main__": 
	for line in sys.stdin:
		print parse_tuple(0, line)

