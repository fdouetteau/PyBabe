
from zipfile import ZipFile, ZIP_DEFLATED
from base import BabeBase
from cStringIO import StringIO


def compress(compress_outstream, inputfile_filename, inarchive_filename):
    myzip = ZipFile(compress_outstream, 'w', ZIP_DEFLATED)
    myzip.write(inputfile_filename, inarchive_filename)
    myzip.close()


def get_content_list(compress_instream, filename):
    myzip = ZipFile(compress_instream, 'r')
    return (myzip, myzip.namelist())


def uncompress(handle, name):
    return StringIO(handle.read(name))


BabeBase.addCompressPushPlugin('zip', ['zip'], compress)
BabeBase.addCompressPullPlugin('zip', ['zip'], get_content_list, uncompress)
