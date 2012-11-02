
from pybabe import Babe
from tests_utils import TestCase
import random
import BaseHTTPServer
import urllib2
from threading import Thread


class TestHTTP(TestCase):
    def setUp(self):
        self.port = random.choice(range(9000,11000))
        server_address = ('', self.port)
        class TestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/STOP":
                    self.send_response(200)
                    self.end_headers()
                    return
                p = self.path.replace('/remote', 'tests')
                ff = open(p, 'rb')
                s = ff.read()
                self.send_response(200)
                self.send_header('Content-type',	'text/csv')
                self.end_headers()
                self.wfile.write(s)
                return

            def log_request(self, code, size=None):
                pass

        class RunServer(Thread):
            def run(self):
                self.httpd = BaseHTTPServer.HTTPServer(server_address=server_address,  RequestHandlerClass=TestHandler)
                while self.keep_running:
                    self.httpd.handle_request()
        self.thread = RunServer()
        self.thread.keep_running = True
        self.thread.start()

    def tearDown(self):
        self.thread.keep_running = False
        try:
            k = urllib2.urlopen("http://localhost:%u/STOP" % self.port, timeout=2)
            k.read()
        except Exception:
            pass
        self.thread.join()
        self.thread = None

    def test_http(self):
        a = Babe().pull(protocol='http', host='localhost', name='Test', filename='remote/test.csv', port=self.port)
        self.assertEquals(a.to_string(), 'foo,bar,f,d\n1,2,3.2,2010/10/02\n3,4,1.2,2011/02/02\n')
