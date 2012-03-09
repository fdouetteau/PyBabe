
from zipfile import ZipFile, ZIP_DEFLATED
from base import BabeBase

def compress(compress_outstream, inputfile_filename, inarchive_filename):
     myzip = ZipFile(compress_outstream, 'w', ZIP_DEFLATED)
     myzip.write(inputfile_filename, inarchive_filename)
     myzip.close()
     
     
BabeBase.addCompressPushPlugin('zip', ['zip'], compress)