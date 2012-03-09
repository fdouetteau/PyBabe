
import tempfile
from zipfile import ZipFile, ZIP_DEFLATED
import os
from charset import UnicodeCSVWriter 
from base import BabeBase, MetaInfo

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
                            
            
    def push(self, filename=None, stream = None, format=None, encoding=None, protocol=None, compress=None, **kwargs):
        metainfo = None
        writer = None
        outstream = None
        compress_format = None
        fileExtension = None
        to_close = []
        if filename: 
            fileBaseName, fileExtension = os.path.splitext(filename) 
            fileExtension = fileExtension.lower()
            if len(fileExtension) > 0:
                fileExtension = fileExtension[1:]
        
        if not format and fileExtension:
            if fileExtension in ['xlsx', 'csv', 'tsv']:
                format = fileExtension 
            else: 
                raise Exception("Unable to guess format") 
                
        if not format: 
            raise Exception("Unable to guess format")
        
        if not format in ['xlsx', 'csv']:
            raise Exception('Unsupported format %s' % format) 
                    
        if compress: 
            compress_baseName, compress_fileExtension = os.path.splitext(compress) 
            compress_fileExtension = compress_fileExtension.lower()[1:]
            if compress_fileExtension in ['zip']: 
                compress_format = compress_fileExtension 
            else:
                raise Exception('Unknown exception format %s' % compress_format)
                
        if not protocol:
            protocol = 'file'
        
        if not (protocol in ['file', 'ftp']):
            raise Exception('Unsupported protocol %s' % protocol)

        ftp = None
        if protocol == 'ftp' and kwargs.get('ftp_early_check', True):  # Fail fast for FTP. 
            from ftplib import FTP
            ftp = FTP()
            ftp.connect(kwargs['host'], kwargs.get('port', None))
            ftp.login(kwargs.get('user', None), kwargs.get('password', None))
            ftp.quit()
            
        # If external protocol or compression, write to a temporary file. 
        if protocol is not "file" or compress:
            outstream = tempfile.NamedTemporaryFile()
            to_close.append(outstream)
        elif stream: 
            outstream = stream
        else: 
            outstream = open(filename, 'wb')
            to_close.append(outstream)
            
        
        if format in ['csv', 'tsv']: 
            if not encoding:
                encoding = 'utf-8'
        else: 
            if encoding:
                raise Exception('Invalid encoding %s for format %s' % (encoding, format))
        
        # Actually write the file. 
        if format == 'xlsx':
            from openpyxl import Workbook
            wb = Workbook(optimized_write = True)
            ws = wb.create_sheet()
            for k in self:
                if isinstance(k, MetaInfo):
                    metainfo = k
                    ws.append(metainfo.names)
                else:
                    ws.append(list(k))
            wb.save(outstream)
        elif format == 'csv':
            for k in self: 
                if isinstance(k, MetaInfo):
                    metainfo = k
                    writer = UnicodeCSVWriter(outstream, dialect=metainfo.dialect, encoding=encoding)
                    writer.writerow(metainfo.names)
                else:
                    writer.writerow(list(k))
        outstream.flush()
        
        # Apply file compression
        if compress_format == "zip": 
            if protocol != 'file':
                compress_file = tempfile.NamedTemporaryFile()
            else:
                compress_file = compress
            myzip = ZipFile(compress_file, 'w', ZIP_DEFLATED)
            myzip.write(outstream.name, filename)
            myzip.close()
            filename = compress
            outstream.close()
            outstream = compress_file
            
        # Apply protocol 
        if protocol == 'ftp': 
            from ftplib import FTP
            ftp = FTP()
            ftp.connect(kwargs['host'], kwargs.get('port', None))
            ftp.login(kwargs.get('user', None), kwargs.get('password', None))
            ftp.storbinary('STOR %s' % filename, open(outstream.name, 'rb'))
            ftp.quit()
        for s in to_close:
            s.close()





               

        
            
        

