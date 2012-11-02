
from tests_utils import TestCase
from pybabe import Babe


class TestMAIL(TestCase):
    s1 = "a,b\n1,2\n"
    s2 = "c,d\n3,toto\n"
    def test_mail(self):
        a = Babe().pull(string=self.s1, source="Table 1", format='csv')
        a = a.pull(string=self.s2, source="Table 2", format='csv')
        a.mail(subject="Test", recipients="florian@douetteau.net", in_body=True)