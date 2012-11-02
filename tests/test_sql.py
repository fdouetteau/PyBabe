
from tests_utils import TestCase, skipUnless, can_execute
from pybabe import Babe

class TestSQLPartition(TestCase):
    s = 'id,value,s\n1,coucou,4\n2,blabla,5\n3,coucou,6\n4,tutu,4\n'

    s2 = 'id,value,s\n1,coucou,5\n5,foo,bar\n'

    sr = 'id,value,s\n1,coucou,5\n2,blabla,5\n3,coucou,6\n4,tutu,4\n5,foo,bar\n'

    @skipUnless(can_execute('sqlite3'),  "Requires sqlite3 installed")
    def test_pushsqlite_partition(self):
        a = Babe().pull(string=self.s, format='csv')
        a = a.typedetect()
        a.push_sql(table='test_table', database_kind='sqlite', database='test.sqlite', drop_table = True, create_table=True)

        a = Babe().pull(string=self.s2, format='csv')
        a = a.typedetect()
        a = a.partition(field='id')
        a.push_sql(table='test_table', database_kind='sqlite', database='test.sqlite', delete_partition=True)

        b = Babe().pull_sql(database_kind='sqlite', database='test.sqlite', table='test_table')
        b = b.sort(field="id")
        self.assertEquals(b.to_string(), self.sr)


class TestSQL(TestCase):
    s = 'id,value,s\n1,coucou,4\n2,blabla,5\n3,coucou,6\n4,tutu,4\n'

    @skipUnless(can_execute('sqlite3'),  "Requires sqlite3 installed")
    def test_pushsqlite(self):
        a = Babe().pull(string=self.s, format='csv')
        a = a.typedetect()
        a.push_sql(table='test_table', database_kind='sqlite', database='test.sqlite', drop_table = True, create_table=True)
        b = Babe().pull_sql(database_kind='sqlite', database='test.sqlite', table='test_table')
        self.assertEquals(b.to_string(), self.s)



    @skipUnless(can_execute('mysql'),  "Requires mysql client")
    def test_mysql(self):
        a = Babe().pull(string=self.s, format='csv')
        a = a.typedetect()
        a.push_sql(table='test_table', database_kind='mysql', user='root', database='pybabe_test', drop_table = True, create_table=True)
        b = Babe().pull_sql(database_kind='mysql', user='root', database='pybabe_test', table='test_table')
        self.assertEquals(b.to_string(), self.s)

    # createdb pybabe_test # required before
    @skipUnless(can_execute('vwload'),  "Requires Vectorwise client")
    def test_vectorwise(self):
        a = Babe().pull(string=self.s, format='csv')
        a = a.typedetect()
        a.push_sql(table='test_table', database_kind='vectorwise', database='pybabe_test', drop_table = True, create_table=True)
        b = Babe().pull_sql(database_kind='vectorwise', database='pybabe_test', table='test_table')
        self.assertEquals(b.to_string(), self.s)


class TestSQLDump(TestCase):
    s = """CREATE TABLE BLABLA;
INSERT INTO `foobar` VALUES (11,435787,'Yes\\r\\nI\\\'m good.','2011-07-03 12:15:44'),(13,242393,'Foo','Bar');
MORE BLABLA;
"""

    s2= """id,number,title,datetime
11,435787,"Yes\r
I'm good.",2011-07-03 12:15:44
13,242393,Foo,Bar
"""

    def test_sqldump(self):
        a = Babe().pull(string=self.s, format='sql', table='foobar', fields=['id', 'number', 'title', 'datetime'])
        self.assertEquals(a.to_string(), self.s2)
