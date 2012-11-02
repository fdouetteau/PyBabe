
from tests_utils import TestCase
from pybabe import Babe

test_csv_content = """foo,bar,f,d\n1,2,3.2,2010/10/02\n3,4,1.2,2011/02/02\n"""


class TestZip(TestCase):
    s = "a,b\n1,2\n3,4\n"

    def test_zip(self):
        a = Babe().pull(string=self.s, format="csv")
        a.push(filename='tests/test.zip')
        b = Babe().pull(filename='tests/test.zip')
        self.assertEquals(b.to_string(), self.s)

    def test_zipread(self):
        babe = Babe()
        a = babe.pull(filename='tests/test_read.zip', name="Test")
        self.assertEquals(a.to_string(), test_csv_content)
