
from pybabe import Babe
from tests_utils import TestCase

class TestExcel(TestCase):

    def test_excel_read_write(self):
        babe = Babe()
        b = babe.pull(filename='tests/test.xlsx', name='Test2').typedetect()
        b = b.mapTo(lambda row: row._replace(Foo=-row.Foo))
        b.push(filename='tests/test2.xlsx')
