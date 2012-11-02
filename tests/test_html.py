
from tests_utils import TestCase
from pybabe import Babe 

class TestHTML(TestCase):
    s = "a,b\n1,2\n"
    def test_html(self):
        a = Babe().pull(string=self.s, format="csv")
        print a.to_string(format="html")
