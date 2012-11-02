
from tests_utils import TestCase
from pybabe import Babe 

class TestJoin(TestCase):
    s1 = "city,country\nParis,FR\nLondon,UK\nLyon,FR\n"
    s2 = "country_code,country_name\nFR,France\nUK,United Kingdom\n"
    s2_bis = "country_code,country_name\nFR,France\n"
    sjoined = "city,country,country_name\nParis,FR,France\nLondon,UK,United Kingdom\nLyon,FR,France\n"
    sjoined_bis = "city,country,country_name\nParis,FR,France\nLondon,UK,\nLyon,FR,France\n"


    def test_join(self):
        a = Babe().pull(string=self.s1, format='csv')
        a = a.join(join_stream=Babe().pull(string=self.s2, format='csv'), key='country', join_key='country_code')
        self.assertEquals(a.to_string(), self.sjoined)

    def test_join_none(self):
        a = Babe().pull(string=self.s1, format='csv')
        a = a.join(join_stream=Babe().pull(string=self.s2_bis, format='csv'), key='country', join_key='country_code', on_error=Babe.ON_ERROR_NONE)
        self.assertEquals(a.to_string(), self.sjoined_bis)

