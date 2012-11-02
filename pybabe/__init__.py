
from base import BabeBase, StreamHeader, StreamFooter, StreamMeta

# Load all builtin plugins
import transform
import types
import minmax
import partition

# Just reference these reflective module once
#to avoid warnings from syntax checkers
only_to_load_1 = [transform, types, minmax, partition]
Babe = BabeBase
StreamHeader = StreamHeader
StreamFooter = StreamFooter
StreamMeta = StreamMeta
