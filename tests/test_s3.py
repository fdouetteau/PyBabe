
from tests_utils import TestCase, can_connect_to_the_net, skipUnless
from pybabe import Babe

class TestS3(TestCase):
    @skipUnless(can_connect_to_the_net(), 'Requires net connection')
    def test_s3(self):
        s = "a,b\n1,2\n3,4\n"
        a = Babe().pull(string=s, format='csv', name='Test')
        a.push(filename='test3.csv', bucket='florian-test', protocol="s3")
        b = Babe().pull(filename='test3.csv', name='Test', bucket='florian-test', protocol="s3")
        self.assertEquals(b.to_string(), s)

    @skipUnless(can_connect_to_the_net(), 'Requires net connection')
    def test_s3_glob(self):
        s = "a,b\n1,2\n3,4\n"
        a = Babe().pull(string=s, format='csv', name='Test')
        a.push(filename='test_glob_4.csv', bucket='florian-test', protocol="s3")
        b = Babe().pull(filename='test_glob_?.csv', name='Test', bucket='florian-test', protocol="s3")
        self.assertEquals(b.to_string(), s)

    @skipUnless(can_connect_to_the_net(), 'Requires net connection')
    def test_s3_glob2(self):
        s = "a,b\n1,2\n3,4\n"
        a = Babe().pull(string=s, format='csv', name='Test')
        a.push(filename='foofoobar/test_glob_4.csv', bucket='florian-test', protocol="s3")
        b = Babe().pull(filename='foofoobar/test_glob_?.csv', name='Test', bucket='florian-test', protocol="s3")
        self.assertEquals(b.to_string(), s)
