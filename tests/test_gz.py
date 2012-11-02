from pybabe import Babe
from tests_utils import TestCase

class TestGZ(TestCase):
    s = 'city,b,c\nPARIS,foo,bar\nLONDON,coucou,salut\n'
    def test_gz(self):
        a = Babe().pull(string=self.s, format='csv', name='Test')
        a.push(filename='test.csv.gz')
        b = Babe().pull(filename='test.csv.gz')
        self.assertEquals(b.to_string(), self.s)