
from pybabe import Babe
from tests_utils import TestCase

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