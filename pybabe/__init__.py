
from base import BabeBase

# Load all builtin plugins
import transform, mapreduce, types, babelog
import minmax, format_txt, twitter, mongo, dedup, sql, partition
import kontagent, geo, useragent, join

        

# Just reference these reflective module once, to avoid warnings from syntax checkers
only_to_load_1 = [transform, mapreduce, types, babelog, 
    minmax, format_txt, twitter, mongo, dedup, sql,
    partition, kontagent, geo, useragent, join]
        
Babe = BabeBase




               

        
            
        

