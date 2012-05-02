
from base import BabeBase

# Load all builtin plugins
import transform, mapreduce, format_csv, format_xlsx, types, babelog, compress_zip, protocol_ftp, protocol_http
import protocol_s3, minmax, format_txt, twitter, mongo, dedup, protocol_buzzdata, format_xls, sql, compress_gz, partition
import kontagent, geo
        

# Just reference these reflective module once, to avoid warnings from syntax checkers
only_to_load_1 = [transform, mapreduce, format_csv, format_xlsx, types, babelog, compress_zip, protocol_ftp, 
    protocol_http, protocol_s3, minmax, format_txt, twitter, mongo, dedup, protocol_buzzdata, format_xls, sql,
    compress_gz, partition, kontagent, geo]
        
class Babe(BabeBase):
    def get_iterator(self, stream, m, v, d):
        b = Babe()
        b.stream = stream
        b.m = m
        b.v = v 
        b.d = d 
        return b





               

        
            
        

