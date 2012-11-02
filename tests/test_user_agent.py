
from pybabe import Babe
from tests_utils import TestCase

class TestUserAgent(TestCase):
    s = "foo,useragent\n1,Mozilla/5.0 (Windows NT 5.1; rv:11.0) Gecko/20100101 Firefox/11.0\n"
    s2= "foo,useragent,os,browser,browser_version\n1,Mozilla/5.0 (Windows NT 5.1; rv:11.0) Gecko/20100101 Firefox/11.0,Windows,Firefox,11.0\n"

    def test_user_agent(self):
        a = Babe().pull(string=self.s, format="csv")
        a = a.user_agent(field="useragent", output_os="os", output_browser="browser", output_browser_version="browser_version")
        self.assertEquals(a.to_string(), self.s2)
