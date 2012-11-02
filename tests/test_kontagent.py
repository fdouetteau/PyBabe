
from tests_utils import TestCase
from pybabe import Babe


class TestKontagent(TestCase):
    def test_load(self):
        start_time = '2012-04-23 11:00'
        end_time = '2012-04-23 12:00'
        a = Babe().pull_kontagent(start_time, end_time, sample_mode=True)
        a = a.head(n=10)
        print a.to_string()

    def test_load_partition(self):
        start_time = '2012-04-23 11:00'
        end_time = '2012-04-23 12:00'
        a = Babe().pull_kontagent(start_time, end_time, sample_mode=True)
        a = a.head(n=10)
        d = {}
        a.push(stream_dict=d, format='csv')
        self.assertEquals(list(d.keys()), ['2012-04-23_11'])
