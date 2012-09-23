
from base import BabeBase, StreamHeader, StreamFooter, StreamMeta

# Load all builtin plugins
import transform
import mapreduce
import types
import babelog
import minmax
import format_txt
import twitter
import mongo
import dedup
import sql
import partition
import geo
import useragent
import join

# Just reference these reflective module once
#to avoid warnings from syntax checkers
only_to_load_1 = [transform, mapreduce, types, babelog,
    minmax, format_txt, twitter, mongo, dedup, sql,
    partition, geo, useragent, join]
Babe = BabeBase
StreamHeader = StreamHeader
StreamFooter = StreamFooter
StreamMeta = StreamMeta
