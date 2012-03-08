
from base import BabeBase, MetaInfo
import itertools
      
def map(stream, column, function):
    return itertools.imap(lambda elt : elt._replace(**{column : function(getattr(elt, column))}) if not isinstance(elt, MetaInfo) else elt,
           stream)
BabeBase.register("map", map)