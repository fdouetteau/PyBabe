from tests_utils import TestCase, skipUnless, can_connect_to_the_net
from pybabe import Babe

class TestTwitter(TestCase):
    @skipUnless(can_connect_to_the_net(), 'Requires net connection')
    def test_twitter(self):
        a = Babe().pull_twitter()
        a = a.filterColumns(keep_fields=
        ["author_name", "author_id", "author_screen_name", "created_at", "hashtags", "text", "in_reply_to_status_id_str"])
        a = a.typedetect()
        a.to_string()
