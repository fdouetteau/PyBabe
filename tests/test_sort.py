
from pybabe import Babe
from tests_utils import TestCase

class TestSort(TestCase):
    def test_sort(self):
        babe = Babe()
        s = '\n'.join(['k,v'] + [ '%u,%u' % (i,-i) for i in xrange(0,10001)])
        a = babe.pull(string=s, name='test', format='csv')
        a = a.typedetect()
        a = a.sort(field='v')
        a = a.head(n=1)
        self.assertEquals(a.to_string(), 'k,v\n10000,-10000\n')

    def test_sortdiskbased(self):
        babe = Babe()
        s = '\n'.join(['k,v'] + [ '%u,%u' % (i,-i) for i in xrange(0,10001)])
        a = babe.pull(string=s, name='test', format='csv')
        a = a.typedetect()
        a = a.sort_diskbased(field='v', nsize=1000)
        a = a.head(n=1)
        self.assertEquals(a.to_string(), 'k,v\n10000,-10000\n')
