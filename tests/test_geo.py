
from pybabe import Babe
from tests_utils import TestCase

class TestGeo(TestCase):
    s = "name,ip\nFlo,82.231.177.189\nFla,4.3.1.432\n"
    s2 = "name,ip,country_code\nFlo,82.231.177.189,FR\nFla,4.3.1.432,US\n"
    def test_country_code(self):
        a = Babe().pull(string=self.s, format='csv')
        a = a.geoip_country_code()
        self.assertEquals(a.to_string(), self.s2)