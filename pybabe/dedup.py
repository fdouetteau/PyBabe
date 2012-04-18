
from base import BabeBase, StreamHeader


def dedup(stream, primary_keys=False, columns = None):
	"""
Deduplicate a stream
If columns is specified only apply the  deduplication on the specified columns
If primary_keys is True only apply the deduplication based on the field marked as "primary in StreamHeader
Otherwise apply the deduplication over all values. 
	"""
	for row in stream:
		if isinstance(row, StreamHeader):
			metainfo = row
			if primary_keys:
				columns = metainfo.primary_keys
			if columns: 
				indexes = [metainfo.names.index(c) for c in columns]
			else:
				indexes = None
			s = set()
			yield row
		else:
			if indexes:
				l = list(row)
				v = tuple([l[i] for i in indexes])
			else:
				v = row
			if v in s: 
				pass
			else: 
				yield row
				s.add(v)


BabeBase.register('dedup', dedup)