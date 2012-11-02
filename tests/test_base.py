
from pybabe import Babe, StreamHeader
from tests_utils import TestCase
from  cStringIO import StringIO 
from tempfile import NamedTemporaryFile
import os


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
        a = a.pull(stream=StringIO(self.s), format='csv').pull(string=self.s, format='csv')
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), self.s+self.s)

    s2 = "a,b\n1,2\n3,4\n1,2\n3,4\n"
    def test_multi2(self):
        a = Babe()
        a = a.pull(stream=StringIO(self.s), format='csv').pull(string=self.s, format='csv')
        a = a.merge_substreams()
        buf = StringIO()
        a.push(stream=buf, format='csv')
        self.assertEquals(buf.getvalue(), self.s2)







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