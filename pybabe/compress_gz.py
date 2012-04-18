from base import BabeBase
from subprocess import Popen, PIPE
import os.path

def compress(compress_outstream, inputfile_filename, inarchive_filename):
	f = open(compress_outstream, 'w')
	p = Popen(['gzip', '-c', inputfile_filename], stdout=f)
	p.communicate()
	f.close()

def get_content_list(compress_instream, filename):
	p = Popen(['gzip', '-d', '-c'], stdin=compress_instream, stdout=PIPE)
	f = os.path.splitext(os.path.basename(filename))[0]
	return (p, [f])
     
def uncompress(handle, name):
	return handle.stdout
    
    
BabeBase.addCompressPushPlugin('gz', ['gz'], compress)
BabeBase.addCompressPullPlugin('gz', ['gz'], get_content_list, uncompress, need_seek=False)