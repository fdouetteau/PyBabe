#!env/bin/python

from pybabe import Babe
from pybabe.base import StreamHeader
from cStringIO import StringIO
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
        a = a.group(key="a", reducer=lambda key, rows: (key, sum([row.b for row in rows])))
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), "a,b\n1,6\n3,4\n")

    def test_groupAll(self):
        a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
        a = a.group_all(reducer=lambda rows: (max([row.b for row in rows]),), fields=['max'])
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

if __name__ == "__main__":
    main()