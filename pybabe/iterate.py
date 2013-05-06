
from base import BabeBase, StreamHeader, StreamFooter, StreamMeta
from collections import *

class Bunch:
  def __init__(self, dictionary):
    self.__dict__ = dictionary

def iterate(stream, function, insert_fields=None, typename=None):
  metainfo = None
  for row in stream:
    if isinstance(row, StreamHeader):
      metainfo = row
      if insert_fields is not None:
        metainfo = metainfo.insert(typename=typename, fields=insert_fields)
      yield metainfo
    elif isinstance(row, StreamMeta):
      yield row
    else:
      d = row._asdict()
#      values = tuple(row)
      if insert_fields is not None:
        for field in insert_fields:
          d[field] = None
#      values = metainfo.t._make(values)
      result = function(Bunch(d))
      yield metainfo.t._make(d.values())
#      yield metainfo.t._make([result.__dict__[key] for key in metainfo.t._fields])

BabeBase.register("iterate", iterate)


