#!env/bin/python

from pybabe import Babe
from pybabe.base import keynormalize
import unittest
import random
from cStringIO import StringIO
from pyftpdlib import ftpserver
from threading import Thread
import shutil, tempfile

class TestBasicFunction(unittest.TestCase):
    def test_pull_push(self):
        babe = Babe()
        a = babe.pull('tests/test.csv', name='Test').typedetect()
        a = a.map('foo', lambda x : -x).multimap({'bar' : lambda x : x + 1, 'f' : lambda f : f / 2 }).sort('foo')
        a = a.groupkey('foo', int.__add__, 0, keepOriginal=True)
        a.push(filename='tests/test2.csv')
        
    def test_keynormalize(self):
        self.assertEqual('Payant_Gratuit', keynormalize('Payant/Gratuit'))
    
    def test_pull_process(self):
        babe = Babe()
        a = babe.pull(command=['/bin/ls', '-1', '.'], name='ls', names=['filename'], format="csv", encoding='utf8')
        a.push(filename='tests/ls.csv')
        
    def test_log(self):
        buf = StringIO()
        buf2 = StringIO()
        babe = Babe()
        a = babe.pull('tests/test.csv', name='Test')
        a = a.log(logfile=buf)
        a.push(stream=buf2, format='csv')
        s = """foo	bar	f	d
1	2	3.2	2010/10/02
3	4	1.2	2011/02/02
"""
        self.assertEqual(s, buf.getvalue())
        self.assertEqual(s, buf2.getvalue())
        
    def test_augment(self):
        babe = Babe()
        a = babe.pull('tests/test.csv', name='Test').typedetect()
        a = a.augment(lambda o: [o.bar], name='Test2', names=['bar2'])
        buf = StringIO()
        a.push(stream=buf, format='csv')
        s = buf.getvalue()
        ss = """foo\tbar\tf\td\tbar2
1\t2\t3.2\t2010-10-02\t2
3\t4\t1.2\t2011-02-02\t4
"""
        self.assertEquals(s, ss)
        
test_csv_content = """foo\tbar\tf\td\n1\t2\t3.2\t2010/10/02\n3\t4\t1.2\t2011/02/02\n"""
        
class TestZip(unittest.TestCase):
    def test_zip(self):
        babe = Babe()
        a = babe.pull('tests/test.csv', name='Test')
        a.push(filename='test.csv', compress='tests/test.zip')
        
    def test_zipread(self):
        babe = Babe()
        a = babe.pull('tests/test_read.zip', name="Test")
        buf = StringIO() 
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), test_csv_content)
        
class TestFTP(unittest.TestCase):
    def setUp(self):
        self.port = random.choice(range(9000,11000))
        authorizer = ftpserver.DummyAuthorizer()
        self.dir = tempfile.mkdtemp()
        self.user = 'user'
        self.password = 'password'
        ftpserver.log = lambda x : None
        ftpserver.logline = lambda x : None
        authorizer.add_user(self.user, self.password, self.dir, perm='elradfmw')
        address = ('127.0.0.1', self.port)
        ftp_handler = ftpserver.FTPHandler
        ftp_handler.authorizer = authorizer 
        self.ftpd = ftpserver.FTPServer(address, ftp_handler)
        class RunServer(Thread):
            def run(self):
                try:
                    self.ftpd.serve_forever()
                except Exception: 
                    pass
        s = RunServer()
        s.ftpd = self.ftpd
        s.start()
 
        
    def tearDown(self):
        self.ftpd.close_all()
        if self.dir.startswith('/tmp'):
            shutil.rmtree(self.dir)
    
    def test_ftp(self):
        babe = Babe()
        a = babe.pull('tests/test.csv', name='Test')
        a.push(filename='test.csv', protocol='ftp', user=self.user, password=self.password, host='localhost', port=self.port, protocol_early_check= False)
        b = babe.pull('test.csv', name='Test', protocol='ftp', user=self.user, password=self.password, host='localhost', port=self.port)
        buf = StringIO()
        b.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), test_csv_content)
        
    def test_ftpzip(self):
        babe = Babe()
        a = babe.pull('tests/test.csv', name='Test')
        a.push(filename='test.csv', compress='test.zip', protocol='ftp', user=self.user, password=self.password, host='localhost', port=self.port, protocol_early_check=False)
        
        
class TestCharset(unittest.TestCase):
    def test_writeutf16(self):
        babe = Babe()
        a = babe.pull('tests/test.csv', name='Test')
        a.push(filename='tests/test_utf16.csv', encoding='utf_16')
        
    def test_cleanup(self):
        babe = Babe()
        a = babe.pull('tests/test_badencoded.csv', utf8_cleanup=True, name='Test')
        a.push(filename='tests/test_badencoded_out.csv')

    def test_cleanup2(self):
        # Test no cleanup
        babe = Babe()
        a = babe.pull('tests/test_badencoded.csv', name='Test')
        a.push(filename='tests/test_badencoded_out2.csv')

class TestSort(unittest.TestCase): 
    def test_sort(self):
        babe = Babe()
        s = '\n'.join(['k,v'] + [ '%u,%u' % (i,-i) for i in xrange(0,100001)])
        a = babe.pull(stream=StringIO(s), name='test', format='csv')
        a = a.typedetect()
        a = a.sort(key='v')
        a = a.head(n=1)
        buf = StringIO()
        a = a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), 'k,v\n100000,-100000\n')        

    def test_sortdiskbased(self):
        babe = Babe()
        s = '\n'.join(['k,v'] + [ '%u,%u' % (i,-i) for i in xrange(0,100001)])
        a = babe.pull(stream=StringIO(s), name='test', format='csv')
        a = a.typedetect()
        a = a.sort_diskbased(key='v', nsize=10000)
        a = a.head(n=1)
        buf = StringIO()
        a = a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), 'k,v\n100000,-100000\n')        

    
class TestExcel(unittest.TestCase):
    
    def test_excel_read_write(self):
        babe = Babe()
        b = babe.pull('tests/test.xlsx', name='Test2').typedetect()
        b = b.map('Foo', lambda x : -x)
        b.push(filename='tests/test2.xlsx')


import code, traceback, signal

def debug(sig, frame):
    """Interrupt running process, and provide a python prompt for
    interactive debugging."""
    d={'_frame':frame}         # Allow access to frame object.
    d.update(frame.f_globals)  # Unless shadowed by global
    d.update(frame.f_locals)

    i = code.InteractiveConsole(d)
    message  = "Signal recieved : entering python shell.\nTraceback:\n"
    message += ''.join(traceback.format_stack(frame))
    i.interact(message)

def listen():
    signal.signal(signal.SIGUSR1, debug)  # Register handler
    
listen()

if __name__ == "__main__":
    unittest.main()