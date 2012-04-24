from base import BabeBase, StreamHeader, StreamFooter

def partition(stream, field): 
	"""Create substream per different value of 'column'"""
	beginning = False
	last_value = None
	header = None
	for row in stream: 
		if isinstance(row, StreamHeader): 
			beginning = True
			header = row
			 
		elif isinstance(row, StreamFooter): 
			yield row 
		else:
			v = getattr(row, field)
			if beginning: 
				beginning = False
				last_value = v
				yield header.replace(partition=v)
			elif v != last_value:
				yield StreamFooter()
				yield header.replace(partition=v)
				last_value = v 
			yield row

BabeBase.register('partition', partition)

