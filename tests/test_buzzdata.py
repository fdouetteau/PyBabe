
from tests_utils import TestCase, can_connect_to_the_net, skipUnless
from pybabe import Babe

class TestBuzzData(TestCase):
    @skipUnless(can_connect_to_the_net(), 'Requires net connection')
    @skipUnless(Babe.has_config('buzzdata', 'api_key'), 'Requires Buzzdata api Key')
    def test_buzzdata(self):
        a = Babe().pull(protocol='buzzdata',
                dataroom='best-city-contest-worldwide-cost-of-living-index',
                uuid='aINAPyLGur4y37yAyCM7w3',
                 username='eiu', format='xls')
        a = a.head(2)
        a.to_string()
