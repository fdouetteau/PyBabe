
from pybabe import Babe, StreamHeader, StreamMeta

def unpivot(stream, common_fields, unpivot_name_field, unpivot_value_field):
	"""Unpivot a table. Keep fields lines, use other as values"""
	for row in stream:
		if isinstance(row, StreamHeader): 
			header = row.replace(fields=common_fields+[unpivot_name_field, unpivot_value_field])
			other_fields = [field for field in  row.fields if not field in common_fields]
			yield header
		elif isinstance(row, StreamMeta):
			yield row
		else:
			commons = [getattr(row, f) for f in common_fields]
			for field in other_fields:
				yield header.t._make(commons + [field, getattr(row, field)])

Babe.register('unpivot', unpivot)