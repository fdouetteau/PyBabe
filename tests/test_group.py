
from pybabe import Babe
from tests_utils import TestCase


class TestGroup(TestCase):
    def test_groupby(self):
        a = Babe().pull(string='a,b\n1,2\n3,4\n1,4\n', format="csv").typedetect()
        a = a.group(key="a", reducer=lambda key, rows: (key, sum([row.b for row in rows])))
        self.assertEquals(a.to_string(), "a,b\n1,6\n3,4\n")

    def test_groupAll(self):
        a = Babe().pull(string='a,b\n1,2\n3,4\n1,4\n', format="csv").typedetect()
        a = a.group_all(reducer=lambda rows: (max([row.b for row in rows]),), fields=['max'])
        self.assertEquals(a.to_string(), "max\n4\n")