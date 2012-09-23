

from base import BabeBase, StreamHeader, StreamFooter
from pymongo import Connection


def push_mongo(instream, db, collection, drop_collection=False, **kwargs):
    """
    Push each row as an object in a mongodb collection
    StreamHeader.get_primary_identifier is used to retrieve a unique identifier for the row.
    """
    connection = Connection(**kwargs)
    db_ = connection[db]
    coll = db_[collection]
    if drop_collection:
        coll.remove()
    for row in instream:
        if isinstance(row, StreamHeader):
            metainfo = row
            count = 1
        elif isinstance(row, StreamFooter):
            pass
        else:
            d = row._asdict()
            count = count + 1
            # Automatically create document URI if necessary.
            if not "_id" in d:
                d["_id"] = metainfo.get_primary_identifier(row, count)
            coll.insert(d)


def pull_mongo(false_stream, db, collection, spec=None, **kwargs):
    """
    Pull objects from mongo as rows
    """
    k = kwargs.copy()
    if 'fields' in k:
        del k['fields']
    if 'typename'in k:
        del k['typename']
    connection = Connection(**k)
    db_ = connection[db]
    coll = db_[collection]
    metainfo = None
    for doc in coll.find(spec, **k):
        if not metainfo:
            fields = kwargs.get('fields', None)
            if not fields:
                fields = [StreamHeader.keynormalize(n) for n in doc]
                fields.sort()  # Mandatory for determisn.
            typename = kwargs.get('typename', collection)
            metainfo = StreamHeader(**dict(kwargs, typename=typename, fields=fields))
            yield metainfo
        yield metainfo.t(*[doc[field] for field in fields])
    if metainfo:
        yield StreamFooter()

BabeBase.registerFinalMethod("push_mongo", push_mongo)
BabeBase.register("pull_mongo", pull_mongo)
