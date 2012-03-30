

from base import BabeBase, MetaInfo
from pymongo import Connection 

def push_mongo(instream, db, collection, **kwargs):
	"""
	Push each row as an object in a mongodb collection
	MetaInfo.get_primary_identifier is used to retrieve a unique identifier for the row. 
	"""
	connection = Connection(**kwargs)
	db_ = connection[db]
	coll = db_[collection]
	for row in instream:
		if isinstance(row, MetaInfo):
			metainfo = row
			count = 1
		else:
			d = row._asdict()
			count = count+1
			d["_id"] = metainfo.get_primary_identifier(row, count)
			coll.insert(d)


def pull_mongo(false_stream, db, collection, spec=None, name=None, names=None, primary_keys = ["id"], **kwargs): 
	"""
	Pull objects from mongo as rows
	"""
	connection = Connection(**kwargs)
	db_ = connection[db]
	coll  = db_[collection]
	metainfo = None
	for doc in coll.find(spec, **kwargs):
		if not metainfo: 
			names = names if names else [MetaInfo.keynormalize(n) for n in doc]
			metainfo = MetaInfo(name=name if name else collection, primary_keys=primary_keys, names=names)
			yield metainfo
		if not 'id' in doc: 
			doc['id'] = doc['_id']
		yield metainfo.t(*[doc[k] for k in names])

BabeBase.registerFinalMethod("push_mongo", push_mongo)
BabeBase.register("pull_mongo", pull_mongo)