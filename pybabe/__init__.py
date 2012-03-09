
from base import BabeBase

# Load all builtin plugins
import transform, mapreduce, format_csv, format_xlsx, types, logging
        
# Just reference these reflective module once, to avoid warnings from syntax checkers
only_to_load_1 = [transform, mapreduce, format_csv, format_xlsx, types, logging]
        
class Babe(BabeBase):
    def get_iterator(self, stream, m, v, d):
        b = Babe()
        b.stream = stream
        b.m = m
        b.v = v 
        b.d = d 
        return b
                            
            





               

        
            
        

