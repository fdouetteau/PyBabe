
from pybabe import Babe
from tests_utils import TestCase
from pyftpdlib import ftpserver
import random
import tempfile
from threading import Thread
import shutil

test_csv_content = """foo,bar,f,d\n1,2,3.2,2010/10/02\n3,4,1.2,2011/02/02\n"""

class TestFTP(TestCase):
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
        a = babe.pull(filename='tests/test.csv', name='Test')
        a.push(filename='test.csv', protocol='ftp', user=self.user, password=self.password, host='localhost', port=self.port, protocol_early_check= False)
        b = babe.pull(filename='test.csv', name='Test', protocol='ftp', user=self.user, password=self.password, host='localhost', port=self.port)
        self.assertEquals(b.to_string(),  test_csv_content)

    def test_ftpzip(self):
        babe = Babe()
        a = babe.pull(filename='tests/test.csv', name='Test')
        a.push(filename='test.csv', compress='test.zip', protocol='ftp', user=self.user, password=self.password, host='localhost', port=self.port, protocol_early_check=False)
