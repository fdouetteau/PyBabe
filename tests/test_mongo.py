
from pybabe import Babe
from tests_utils import TestCase, can_connect, skipUnless

class TestMongo(TestCase):
    s1 = 'rown,f,s\n1,4.3,coucou\n2,4.2,salut\n'
    s2 = 'rown,f,s\n1,4.3,coucou2\n2,4.2,salut2\n'

    @skipUnless(can_connect("localhost", 27017), "Requires Mongo localhost instance running")
    def test_push(self):
        a  = Babe().pull(string=self.s1, format='csv', primary_key='rown')
        a = a.typedetect()
        a.push_mongo(db='pybabe_test',collection='test_push')


    @skipUnless(can_connect("localhost", 27017), "Requires Mongo localhost instance running")
    def test_pushpull(self):
        a  = Babe().pull(string=self.s2, format='csv', primary_key='rown')
        a = a.typedetect()
        a.push_mongo(db='pybabe_test',collection='test_pushpull', drop_collection=True)
        b = Babe().pull_mongo(db="pybabe_test", fields=['rown', 'f', 's'], collection='test_pushpull')
        self.assertEquals(b.to_string(), self.s2)
