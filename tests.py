#!env/bin/python

from pybabe import Babe
from pybabe.base import StreamHeader
import random
from cStringIO import StringIO
from pyftpdlib import ftpserver
from threading import Thread
import shutil
import tempfile
import BaseHTTPServer
import urllib2
from tempfile import NamedTemporaryFile
import os
import socket
try:
    from unittest import TestCase, skipUnless, main
except:
    from unittest2 import TestCase, skipUnless, main

def can_connect_to_the_net():
    try:
        socket.gethostbyname('www.google.com')
        return True
    except Exception:
        return False

def can_execute(s):
    try:
        from subprocess import Popen, PIPE
        p = Popen([s], stdin=PIPE, stdout=PIPE, stderr=PIPE)
        p.stdin.close()
        p.wait()
        return True
    except OSError:
        return False

def can_connect(host, port):
    try:
        socket.create_connection((host, port), timeout=1)
        return True
    except Exception:
        return False


class TestBasicFunction(TestCase):

    def test_keynormalize(self):
        self.assertEqual('Payant_Gratuit', StreamHeader.keynormalize('Payant/Gratuit'))

    def test_pull_process(self):
        babe = Babe()
        a = babe.pull(command=['/bin/ls', '-1', '.'], source='ls', fields=['filename'], format="csv", encoding='utf8')
        a.push(filename='tests/ls.csv')

    def test_log(self):
        buf = StringIO()
        buf2 = StringIO()
        babe = Babe()
        a = babe.pull(filename='tests/test.csv', source='Test')
        a = a.log(logfile=buf)
        a.push(stream=buf2, format='csv')
        s = """foo,bar,f,d
1,2,3.2,2010/10/02
3,4,1.2,2011/02/02
"""
        self.assertEqual(s, buf.getvalue())
        self.assertEqual(s, buf2.getvalue())


class TestMultiPull(TestCase):
    s = "a,b\n1,2\n3,4\n"
    def test_multi(self):
        a = Babe()
        a = a.pull(stream=StringIO(self.s), format='csv').pull(stream=StringIO(self.s), format='csv')
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), self.s+self.s)

    s2 = "a,b\n1,2\n3,4\n1,2\n3,4\n"
    def test_multi2(self):
        a = Babe()
        a = a.pull(stream=StringIO(self.s), format='csv').pull(stream=StringIO(self.s), format='csv')
        a = a.merge_substreams()
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), self.s2)


test_csv_content = """foo,bar,f,d\n1,2,3.2,2010/10/02\n3,4,1.2,2011/02/02\n"""

class TestZip(TestCase):
    s = "a,b\n1,2\n3,4\n"
    def test_zip(self):
        babe = Babe()
        a = babe.pull(stream=StringIO(self.s), format="csv")
        a.push(filename='tests/test.zip')
        b = Babe().pull(filename='tests/test.zip')
        buf = StringIO()
        b.push(stream=buf)
        self.assertEquals(buf.getvalue(), self.s)


    def test_zipread(self):
        babe = Babe()
        a = babe.pull(filename='tests/test_read.zip', name="Test")
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), test_csv_content)

class TestGZ(TestCase):
    s = 'city,b,c\nPARIS,foo,bar\nLONDON,coucou,salut\n'
    def test_gz(self):
        a = Babe().pull(stream=StringIO(self.s), format='csv', name='Test')
        a.push(filename='test.csv.gz')
        b = Babe().pull(filename='test.csv.gz')
        buf = StringIO()
        b.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), self.s)

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
        buf = StringIO()
        b.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), test_csv_content)

    def test_ftpzip(self):
        babe = Babe()
        a = babe.pull(filename='tests/test.csv', name='Test')
        a.push(filename='test.csv', compress='test.zip', protocol='ftp', user=self.user, password=self.password, host='localhost', port=self.port, protocol_early_check=False)


class TestCharset(TestCase):
    def test_writeutf16(self):
        babe = Babe()
        a = babe.pull(filename='tests/test.csv', name='Test')
        a.push(filename='tests/test_utf16.csv', encoding='utf_16')

    def test_cleanup(self):
        babe = Babe()
        a = babe.pull(filename='tests/test_badencoded.csv', utf8_cleanup=True, name='Test')
        a.push(filename='tests/test_badencoded_out.csv')

    def test_cleanup2(self):
        # Test no cleanup
        babe = Babe()
        a = babe.pull(filename='tests/test_badencoded.csv', name='Test')
        a.push(filename='tests/test_badencoded_out2.csv')

class TestSort(TestCase):
    def test_sort(self):
        babe = Babe()
        s = '\n'.join(['k,v'] + [ '%u,%u' % (i,-i) for i in xrange(0,100001)])
        a = babe.pull(stream=StringIO(s), name='test', format='csv')
        a = a.typedetect()
        a = a.sort(field='v')
        a = a.head(n=1)
        buf = StringIO()
        a = a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), 'k,v\n100000,-100000\n')

    def test_sortdiskbased(self):
        babe = Babe()
        s = '\n'.join(['k,v'] + [ '%u,%u' % (i,-i) for i in xrange(0,100001)])
        a = babe.pull(stream=StringIO(s), name='test', format='csv')
        a = a.typedetect()
        a = a.sort_diskbased(field='v', nsize=10000)
        a = a.head(n=1)
        buf = StringIO()
        a = a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), 'k,v\n100000,-100000\n')


class TestExcel(TestCase):

    def test_excel_read_write(self):
        babe = Babe()
        b = babe.pull(filename='tests/test.xlsx', name='Test2').typedetect()
        b = b.mapTo(lambda row: row._replace(Foo=-row.Foo))
        b.push(filename='tests/test2.xlsx')


class TestTransform(TestCase):
    def test_split(self):
        babe = Babe()
        buf = StringIO("""a,b
1,3:4
2,7
""")
        a = babe.pull(stream=buf,format='csv',name='Test')
        a = a.split(field='b',separator=':')
        buf2 = StringIO()
        a.push(stream=buf2, format='csv')
        self.assertEquals(buf2.getvalue(), """a,b
1,3
1,4
2,7
""")

    s = 'city,b,c\nPARIS,foo,bar\nLONDON,coucou,salut\n'
    s2 = 'field,PARIS,LONDON\nb,foo,coucou\nc,bar,salut\n'
    def test_transpose(self):
        a = Babe().pull(stream=StringIO(self.s), format='csv', primary_key='city').transpose()
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), self.s2)

    sr ='city,b,c\nPARIS,foo,bar\nLONDON,barbar,salut\n'
    def test_replace(self):
        a = Babe().pull(stream=StringIO(self.s), format="csv")
        a = a.replace_in_string("cou", "bar", field="b")
        buf = StringIO()
        a.push(stream=buf, format="csv")
        self.assertEquals(buf.getvalue(), self.sr)


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
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), 'foo,bar,f,d\n1,2,3.2,2010/10/02\n3,4,1.2,2011/02/02\n')


class TestS3(TestCase):
    @skipUnless(can_connect_to_the_net(), 'Requires net connection')
    def test_s3(self):
        s = "a,b\n1,2\n3,4\n"
        buf1 = StringIO(s)
        a = Babe().pull(stream=buf1, format='csv', name='Test')
        a.push(filename='test3.csv', bucket='florian-test', protocol="s3")
        b = Babe().pull(filename='test3.csv', name='Test', bucket='florian-test', protocol="s3")
        buf = StringIO()
        b.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), s)

    @skipUnless(can_connect_to_the_net(), 'Requires net connection')
    def test_s3_glob(self):
        s = "a,b\n1,2\n3,4\n"
        buf1 = StringIO(s)
        a = Babe().pull(stream=buf1, format='csv', name='Test')
        a.push(filename='test_glob_4.csv', bucket='florian-test', protocol="s3")
        b = Babe().pull(filename='test_glob_?.csv', name='Test', bucket='florian-test', protocol="s3")
        buf = StringIO()
        b.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), s)

    @skipUnless(can_connect_to_the_net(), 'Requires net connection')
    def test_s3_glob2(self):
        s = "a,b\n1,2\n3,4\n"
        buf1 = StringIO(s)
        a = Babe().pull(stream=buf1, format='csv', name='Test')
        a.push(filename='foofoobar/test_glob_4.csv', bucket='florian-test', protocol="s3")
        b = Babe().pull(filename='foofoobar/test_glob_?.csv', name='Test', bucket='florian-test', protocol="s3")
        buf = StringIO()
        b.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), s)


class TestMapTo(TestCase):
    def test_tuple(self):
        a = Babe().pull(filename='tests/test.csv', name='Test').typedetect()
        a = a.mapTo(lambda obj: obj._replace(foo=obj.foo + 1))
        buf = StringIO()
        a.push(stream=buf, format='csv')
        s = """foo,bar,f,d
2,2,3.2,2010/10/02
4,4,1.2,2011/02/02
"""
        self.assertEquals(buf.getvalue(), s)

    s = "a\n1\n2\n3\n4\n"
    s2 = "a,b\n1,3\n2,3\n3,7\n4,7\n"

    def test_bulk(self):
        a = Babe().pull(stream=StringIO(self.s), format="csv")
        a = a.typedetect()
        a = a.bulkMapTo(lambda list: [[sum([r.a for r in list])]] * len(list), bulk_size=2, insert_fields=["b"])
        buf = StringIO()
        a.push(stream=buf, format="csv")
        self.assertEquals(buf.getvalue(), self.s2)

    def test_insert(self):
        a = Babe().pull(filename='tests/test.csv', name='Test').typedetect()
        a = a.mapTo(lambda row : row.foo+1, insert_fields=['fooplus'])
        buf = StringIO()
        a.push(stream=buf, format='csv')
        s = """foo,bar,f,d,fooplus
1,2,3.2,2010/10/02,2
3,4,1.2,2011/02/02,4
"""
        self.assertEquals(buf.getvalue(), s)

    def test_replace(self):
        a = Babe().pull(filename='tests/test.csv', name='Test').typedetect()
        a = a.mapTo(lambda row : [row.foo+1, row.bar*2], fields=['a','b'])
        buf = StringIO()
        a.push(stream=buf, format='csv')
        s = """a,b\n2,4\n4,8\n"""
        self.assertEquals(buf.getvalue(), s)


class TestFlatMap(TestCase):
    def test_tuple(self):
        a = Babe().pull(stream=StringIO("a,b\n1,2:3\n4,5:6\n"), format="csv")
        a = a.flatMap(lambda row: [row._replace(b=i) for i in row.b.split(':')])
        buf = StringIO()
        a.push(stream=buf, format="csv")
        self.assertEquals(buf.getvalue(), "a,b\n1,2\n1,3\n4,5\n4,6\n")


class TestGroup(TestCase):
    def test_groupby(self):
        a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
        a = a.groupBy(key="a", reducer=lambda key, rows: (key, sum([row.b for row in rows])))
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), "a,b\n1,6\n3,4\n")

    def test_groupAll(self):
        a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
        a = a.groupAll(reducer=lambda rows: (max([row.b for row in rows]),), fields=['max'])
        buf = StringIO()
        a.push(stream=buf, format="csv")
        self.assertEquals(buf.getvalue(), "max\n4\n")


class TestFilterColumns(TestCase):
    def test_filter(self):
        a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
        a = a.filterColumns(keep_fields=['a'])
        buf = StringIO()
        a.push(stream=buf, format="csv")
        self.assertEquals(buf.getvalue(), "a\n1\n3\n1\n")

    def test_filter2(self):
         a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
         a = a.filterColumns(remove_fields=['a'])
         buf = StringIO()
         a.push(stream=buf, format="csv")
         self.assertEquals(buf.getvalue(), "b\n2\n4\n4\n")


class TestFilter(TestCase):
    def test_filter(self):
        a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
        a = a.filter(function=lambda x : x.a == 3)
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), 'a,b\n3,4\n')


    def test_filter_values(self):
        a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
        a = a.filter_values(a=3,b=4)
        buf = StringIO()
        a.push(stream=buf, format="csv")
        self.assertEquals(buf.getvalue(), "a,b\n3,4\n")
    #def test_groupby_sum(self):
    #    a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
    #    a = a.groupBy(key="a", reducer=lambda rows: rows + rows[0]._replace(b=sum([row.b for row in rows])))
    #    buf = StringIO()
    #    a.push(stream=buf, format='csv')
    #    self.assertEquals(buf.getvalue(), "a,b,sum\n1,2,\n1,4,\n,,6\n3,4,\n,,4\n")


class TestMinMax(TestCase):
    def test_max(self):
        a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
        a = a.maxN(column='b', n=2)
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), 'a,b\n3,4\n1,4\n')

    def test_min(self):
        a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
        a = a.minN(column='a', n=2)
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), 'a,b\n1,2\n1,4\n')


class TestRename(TestCase):
    def test_rename(self):
        a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
        a = a.rename(a="c")
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), 'c,b\n1,2\n3,4\n1,4\n')


class TestWindowMap(TestCase):
    def test_windowMap(self):
        a = Babe().pull(stream=StringIO('a\n1\n2\n3\n4\n5\n6\n7\n'), format="csv").typedetect()
        a = a.windowMap(3, lambda rows: rows[-1]._make([sum([row.a for row in rows])]))
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), 'a\n1\n3\n6\n9\n12\n15\n18\n')


class TestTwitter(TestCase):
    @skipUnless(can_connect_to_the_net(), 'Requires net connection')
    def test_twitter(self):
        a = Babe().pull_twitter()
        a = a.filterColumns(keep_fields=
        ["author_name", "author_id", "author_screen_name", "created_at", "hashtags", "text", "in_reply_to_status_id_str"])
        a = a.typedetect()
        buf = StringIO()
        a.push(stream=buf, format='csv')

class TestMongo(TestCase):
    s1 = 'rown,f,s\n1,4.3,coucou\n2,4.2,salut\n'
    s2 = 'rown,f,s\n1,4.3,coucou2\n2,4.2,salut2\n'

    @skipUnless(can_connect("localhost", 27017), "Requires Mongo localhost instance running")
    def test_push(self):
        a  = Babe().pull(stream=StringIO(self.s1), format='csv', primary_key='rown')
        a = a.typedetect()
        a.push_mongo(db='pybabe_test',collection='test_push')


    @skipUnless(can_connect("localhost", 27017), "Requires Mongo localhost instance running")
    def test_pushpull(self):
        a  = Babe().pull(stream=StringIO(self.s2), format='csv', primary_key='rown')
        a = a.typedetect()
        a.push_mongo(db='pybabe_test',collection='test_pushpull', drop_collection=True)
        b = Babe().pull_mongo(db="pybabe_test", fields=['rown', 'f', 's'], collection='test_pushpull')
        buf = StringIO()
        b.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), self.s2)

class TestDedup(TestCase):
    s = 'id,value,s\n1,coucou,4\n2,blabla,5\n3,coucou,6\n4,tutu,4\n'
    s2 = 'id,value,s\n1,coucou,4\n1,coucou,4\n3,coucou,6\n4,tutu,4\n'
    s3 = 'id,value,s\n1,coucou,4\n3,coucou,6\n4,tutu,4\n'
    s4 = 'id,value,s\n1,coucou,4\n2,blabla,5\n4,tutu,4\n'

    def test_dedup1(self):
        a = Babe().pull(stream=StringIO(self.s), format="csv")
        a = a.dedup()
        buf = StringIO()
        a.push(stream=buf,format="csv")
        self.assertEquals(buf.getvalue(), self.s)

    def test_dedup2(self):
        a = Babe().pull(stream=StringIO(self.s2), format="csv")
        a = a.dedup()
        buf = StringIO()
        a.push(stream=buf,format="csv")
        self.assertEquals(buf.getvalue(), self.s3)

    def test_dedup3(self):
        a = Babe().pull(stream=StringIO(self.s2), format="csv")
        a = a.dedup(fields=['id'])
        buf = StringIO()
        a.push(stream=buf,format="csv")
        self.assertEquals(buf.getvalue(), self.s3)

    def test_dedup4(self):
        a = Babe().pull(stream=StringIO(self.s), format="csv")
        a = a.dedup(fields=['value'])
        buf = StringIO()
        a.push(stream=buf,format="csv")
        self.assertEquals(buf.getvalue(), self.s4)





class TestPrimaryKey(TestCase):
    s = 'id,value,s\n1,coucou,4\n2,blabla,5\n3,coucou,6\n4,tutu,4\n'

    s2 = 'id,value,s\n1,coucou,4\n2,blabla,5\n3,coucou,6\n4,tutu,7\n'

    s3 = 'id,value,s\n1,coucou,4\n2,blabla,5\n3,coucou,6\n1,tutu,4\n'

    def test_primarykey(self):
        a = Babe().pull(stream=StringIO(self.s), format='csv')
        a = a.primary_key_detect()
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), self.s)

    def test_primarykey2(self):
        a = Babe().pull(stream=StringIO(self.s2), format='csv')
        a = a.primary_key_detect()
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), self.s2)

    def test_primarykey3(self):
        a = Babe().pull(stream=StringIO(self.s3), format='csv')
        a = a.primary_key_detect()
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), self.s3)

    def test_airport(self):
        a = Babe().pull(filename='data/airports.csv')
        a = a.primary_key_detect()
        a = a.head(n=10)
        buf = StringIO()
        a.push(stream=buf, format='csv')

class TestBuzzData(TestCase):
    @skipUnless(can_connect_to_the_net(), 'Requires net connection')
    @skipUnless(Babe.has_config('buzzdata', 'api_key'), 'Requires Buzzdata api Key')
    def test_buzzdata(self):
        a = Babe().pull(protocol='buzzdata',
                dataroom='best-city-contest-worldwide-cost-of-living-index',
                uuid='aINAPyLGur4y37yAyCM7w3',
                 username='eiu', format='xls')
        a = a.head(2)
        buf = StringIO()
        a.push(stream=buf, format='csv')


class TestSQLPartition(TestCase):
    s = 'id,value,s\n1,coucou,4\n2,blabla,5\n3,coucou,6\n4,tutu,4\n'

    s2 = 'id,value,s\n1,coucou,5\n5,foo,bar\n'

    sr = 'id,value,s\n1,coucou,5\n2,blabla,5\n3,coucou,6\n4,tutu,4\n5,foo,bar\n'

    @skipUnless(can_execute('sqlite3'),  "Requires sqlite3 installed")
    def test_pushsqlite_partition(self):
        a = Babe().pull(stream=StringIO(self.s), format='csv')
        a = a.typedetect()
        a.push_sql(table='test_table', database_kind='sqlite', database='test.sqlite', drop_table = True, create_table=True)

        a = Babe().pull(stream=StringIO(self.s2), format='csv')
        a = a.typedetect()
        a = a.partition(field='id')
        a.push_sql(table='test_table', database_kind='sqlite', database='test.sqlite', delete_partition=True)

        b = Babe().pull_sql(database_kind='sqlite', database='test.sqlite', table='test_table')
        b = b.sort(field="id")
        buf = StringIO()
        b.push(stream=buf, format='csv', delimiter=',')
        self.assertEquals(buf.getvalue(), self.sr)


class TestSQL(TestCase):
    s = 'id,value,s\n1,coucou,4\n2,blabla,5\n3,coucou,6\n4,tutu,4\n'

    @skipUnless(can_execute('sqlite3'),  "Requires sqlite3 installed")
    def test_pushsqlite(self):
        a = Babe().pull(stream=StringIO(self.s), format='csv')
        a = a.typedetect()
        a.push_sql(table='test_table', database_kind='sqlite', database='test.sqlite', drop_table = True, create_table=True)
        b = Babe().pull_sql(database_kind='sqlite', database='test.sqlite', table='test_table')
        buf = StringIO()
        b.push(stream=buf, format='csv', delimiter=',')
        self.assertEquals(buf.getvalue(), self.s)



    @skipUnless(can_execute('mysql'),  "Requires mysql client")
    def test_mysql(self):
        a = Babe().pull(stream=StringIO(self.s), format='csv')
        a = a.typedetect()
        a.push_sql(table='test_table', database_kind='mysql', user='root', database='pybabe_test', drop_table = True, create_table=True)
        b = Babe().pull_sql(database_kind='mysql', user='root', database='pybabe_test', table='test_table')
        buf = StringIO()
        b.push(stream=buf, format='csv', delimiter=',')
        self.assertEquals(buf.getvalue(), self.s)

    # createdb pybabe_test # required before
    @skipUnless(can_execute('vwload'),  "Requires Vectorwise client")
    def test_vectorwise(self):
        a = Babe().pull(stream=StringIO(self.s), format='csv')
        a = a.typedetect()
        a.push_sql(table='test_table', database_kind='vectorwise', database='pybabe_test', drop_table = True, create_table=True)
        b = Babe().pull_sql(database_kind='vectorwise', database='pybabe_test', table='test_table')
        buf = StringIO()
        b.push(stream=buf, format='csv', delimiter=',')
        self.assertEquals(buf.getvalue(), self.s)


class TestMemoize(TestCase):
    s = 'id,value,s\n1,coucou,4\n2,blabla,5\n3,coucou,6\n4,tutu,4\n'
    def test_memo(self):
        tmpfile = NamedTemporaryFile()
        tmpfile.write(self.s)
        tmpfile.flush()
        a = Babe().pull(filename=tmpfile.name, memoize=True, format="csv")
        buf = StringIO()
        a.push(stream=buf, format="csv")
        self.assertEquals(buf.getvalue(), self.s)
        #os.remove(tmpfile.name)
        tmpfile.close()
        self.assertFalse(os.path.exists(tmpfile.name))
        b = Babe().pull(filename=tmpfile.name, memoize=True, format="csv")
        buf2 = StringIO()
        b.push(stream=buf2, format="csv")
        self.assertEquals(buf2.getvalue(), self.s)
        c = Babe().pull(filename=tmpfile.name, memoize=False, format="csv")
        buf3 = StringIO()
        self.assertRaises(IOError, lambda : c.push(stream=buf3, format="csv"))

class TestPartition(TestCase):
    s = 'date,name,value\n2012-04-04,John,1\n2012-04-04,Luke,2\n2012-04-05,John,1\n'

    def test_partition(self):
        a = Babe().pull(stream=StringIO(self.s), format='csv')
        a = a.partition(field = 'date')
        d = {}
        a.push(stream_dict=d, format="csv")
        self.assertEquals(d['2012-04-04'].getvalue(), 'date,name,value\n2012-04-04,John,1\n2012-04-04,Luke,2\n')
        self.assertEquals(d['2012-04-05'].getvalue(), 'date,name,value\n2012-04-05,John,1\n')

    def test_partition_s3(self):
        a = Babe().pull(stream=StringIO(self.s), format='csv')
        a = a.partition(field = 'date')
        a.push(protocol="s3", bucket="florian-test", format="csv", filename_template='foobar/$date.csv.gz')

class TestKontagent(TestCase):
    def test_load(self):
        start_time = '2012-04-23 11:00'
        end_time = '2012-04-23 12:00'
        a = Babe().pull_kontagent(start_time, end_time, sample_mode=True)
        buf = StringIO()
        a = a.head(n=10)
        a.push(stream=buf, format='csv')
        print buf.getvalue()

    def test_load_partition(self):
        start_time = '2012-04-23 11:00'
        end_time = '2012-04-23 12:00'
        a = Babe().pull_kontagent(start_time, end_time, sample_mode=True)
        a = a.head(n=10)
        d = {}
        a.push(stream_dict=d, format='csv')
        self.assertEquals(list(d.keys()), ['2012-04-23_11'])


class TestGeo(TestCase):
    s = "name,ip\nFlo,82.231.177.189\nFla,4.3.1.432\n"
    s2 = "name,ip,country_code\nFlo,82.231.177.189,FR\nFla,4.3.1.432,US\n"
    def test_country_code(self):
        a = Babe().pull(stream=StringIO(self.s), format='csv')
        a = a.geoip_country_code()
        buf = StringIO()
        a.push(stream=buf, format="csv")
        self.assertEquals(buf.getvalue(), self.s2)

class TestNullValue(TestCase):
    s = "foo,bar\n1,2\n2,NULL\n"
    s2 = "foo,bar\n1,2\n2,\n"
    def test_null(self):
        a = Babe().pull(stream=StringIO(self.s), format='csv', null_value="NULL")
        buf = StringIO()
        a = a.push(stream=buf, format="csv")
        self.assertEquals(buf.getvalue(), self.s2)

class TestDate(TestCase):
    s = "foo,time\n1,2012-04-03 00:33\n"
    s2 = "foo,time,date,hour\n1,2012-04-02 22:33:00,2012-04-02,22\n"
    def test_parse(self):
        a = Babe().pull(stream=StringIO(self.s), format='csv')
        buf = StringIO()
        a = a.parse_time(field="time", output_time="time", output_date="date", output_hour="hour", input_timezone="CET", output_timezone="GMT")
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), self.s2)

class TestUserAgent(TestCase):
    s = "foo,useragent\n1,Mozilla/5.0 (Windows NT 5.1; rv:11.0) Gecko/20100101 Firefox/11.0\n"
    s2= "foo,useragent,os,browser,browser_version\n1,Mozilla/5.0 (Windows NT 5.1; rv:11.0) Gecko/20100101 Firefox/11.0,Windows,Firefox,11.0\n"

    def test_user_agent(self):
        a = Babe().pull(stream=StringIO(self.s), format="csv")
        buf = StringIO()
        a = a.user_agent(field="useragent", output_os="os", output_browser="browser", output_browser_version="browser_version")
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), self.s2)


class TestSQLDump(TestCase):
    s = """CREATE TABLE BLABLA;
INSERT INTO `foobar` VALUES (11,435787,'Yes\\r\\nI\\\'m good.','2011-07-03 12:15:44'),(13,242393,'Foo','Bar');
MORE BLABLA;
"""

    s2= """id,number,title,datetime
11,435787,"Yes\r
I'm good.",2011-07-03 12:15:44
13,242393,Foo,Bar
"""

    def test_sqldump(self):
        a = Babe().pull(stream=StringIO(self.s), format='sql', table='foobar', fields=['id', 'number', 'title', 'datetime'])
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), self.s2)

class TestJoin(TestCase):
    s1 = "city,country\nParis,FR\nLondon,UK\nLyon,FR\n"
    s2 = "country_code,country_name\nFR,France\nUK,United Kingdom\n"
    s2_bis = "country_code,country_name\nFR,France\n"
    sjoined = "city,country,country_name\nParis,FR,France\nLondon,UK,United Kingdom\nLyon,FR,France\n"
    sjoined_bis = "city,country,country_name\nParis,FR,France\nLondon,UK,\nLyon,FR,France\n"


    def test_join(self):
        a = Babe().pull(stream=StringIO(self.s1), format='csv')
        a = a.join(join_stream=Babe().pull(stream=StringIO(self.s2), format='csv'), key='country', join_key='country_code')
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), self.sjoined)

    def test_join_none(self):
        a = Babe().pull(stream=StringIO(self.s1), format='csv')
        a = a.join(join_stream=Babe().pull(stream=StringIO(self.s2_bis), format='csv'), key='country', join_key='country_code', on_error=Babe.ON_ERROR_NONE)
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), self.sjoined_bis)

class TestTee(TestCase):
    s = "a,b\n1,2\n"
    def test_tee(self):
        a = Babe().pull(stream=StringIO(self.s), format="csv")
        [b, c] = a.tee(2)
        self.assertEquals(len(b.to_list()), 1)
        self.assertEquals(len(c.to_list()), 1)

class TestHTML(TestCase):
    s = "a,b\n1,2\n"
    def test_html(self):
        a = Babe().pull(stream=StringIO(self.s), format="csv")
        buf = StringIO()
        a.push(stream=buf, format="html")
        print buf.getvalue()

class TestMAIL(TestCase):
    s1 = "a,b\n1,2\n"
    s2 = "c,d\n3,toto\n"
    def test_mail(self):
        a = Babe().pull(stream=StringIO(self.s1), source="Table 1", format='csv')
        a = a.pull(stream=StringIO(self.s2), source="Table 2", format='csv')
        a.mail(subject="Test", recipients="florian@douetteau.net", in_body=True)

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
    main()