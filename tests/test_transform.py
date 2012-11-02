
from pybabe import Babe
from tests_utils import TestCase
from cStringIO import StringIO

class TestTransform(TestCase):
    def test_split(self):
        babe = Babe()
        s = """a,b
1,3:4
2,7
"""
        a = babe.pull(string=s,format='csv',name='Test')
        a = a.split(field='b',separator=':')
        self.assertEquals(a.to_string(), """a,b
1,3
1,4
2,7
""")

    s = 'city,b,c\nPARIS,foo,bar\nLONDON,coucou,salut\n'
    s2 = 'field,PARIS,LONDON\nb,foo,coucou\nc,bar,salut\n'
    def test_transpose(self):
        a = Babe().pull(string=self.s, format='csv', primary_key='city').transpose()
        self.assertEquals(a.to_string(), self.s2)

    sr ='city,b,c\nPARIS,foo,bar\nLONDON,barbar,salut\n'
    def test_replace(self):
        a = Babe().pull(stream=StringIO(self.s), format="csv")
        a = a.replace_in_string("cou", "bar", field="b")
        self.assertEquals(a.to_string(), self.sr)




class TestMapTo(TestCase):
    def test_tuple(self):
        a = Babe().pull(filename='tests/test.csv', name='Test').typedetect()
        a = a.mapTo(lambda obj: obj._replace(foo=obj.foo + 1))
        s = """foo,bar,f,d
2,2,3.2,2010/10/02
4,4,1.2,2011/02/02
"""
        self.assertEquals(a.to_string(),  s)

    s = "a\n1\n2\n3\n4\n"
    s2 = "a,b\n1,3\n2,3\n3,7\n4,7\n"

    def test_bulk(self):
        a = Babe().pull(stream=StringIO(self.s), format="csv")
        a = a.typedetect()
        a = a.bulkMapTo(lambda list: [[sum([r.a for r in list])]] * len(list), bulk_size=2, insert_fields=["b"])
        self.assertEquals(a.to_string(), self.s2)

    def test_insert(self):
        a = Babe().pull(filename='tests/test.csv', name='Test').typedetect()
        a = a.mapTo(lambda row : row.foo+1, insert_fields=['fooplus'])
        s = """foo,bar,f,d,fooplus
1,2,3.2,2010/10/02,2
3,4,1.2,2011/02/02,4
"""
        self.assertEquals(a.to_string(), s)

    def test_replace(self):
        a = Babe().pull(filename='tests/test.csv', name='Test').typedetect()
        a = a.mapTo(lambda row : [row.foo+1, row.bar*2], fields=['a','b'])
        s = """a,b\n2,4\n4,8\n"""
        self.assertEquals(a.to_string(), s)


class TestFlatMap(TestCase):
    def test_tuple(self):
        a = Babe().pull(stream=StringIO("a,b\n1,2:3\n4,5:6\n"), format="csv")
        a = a.flatMap(lambda row: [row._replace(b=i) for i in row.b.split(':')])
        self.assertEquals(a.to_string(), "a,b\n1,2\n1,3\n4,5\n4,6\n")




class TestFilterColumns(TestCase):
    def test_filter(self):
        a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
        a = a.filterColumns(keep_fields=['a'])
        self.assertEquals(a.to_string(), "a\n1\n3\n1\n")

    def test_filter2(self):
         a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
         a = a.filterColumns(remove_fields=['a'])
         self.assertEquals(a.to_string(), "b\n2\n4\n4\n")


class TestFilter(TestCase):
    def test_filter(self):
        a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
        a = a.filter(function=lambda x : x.a == 3)
        self.assertEquals(a.to_string(), 'a,b\n3,4\n')


    def test_filter_values(self):
        a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
        a = a.filter_values(a=3,b=4)
        self.assertEquals(a.to_string(), "a,b\n3,4\n")
    #def test_groupby_sum(self):
    #    a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
    #    a = a.groupBy(key="a", reducer=lambda rows: rows + rows[0]._replace(b=sum([row.b for row in rows])))
    #    buf = StringIO()
    #    a.push(stream=buf, format='csv')
    #    self.assertEquals(buf.getvalue(), "a,b,sum\n1,2,\n1,4,\n,,6\n3,4,\n,,4\n")


class TestMinMax(TestCase):
    def test_max(self):
        a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
        a = a.maxN(column='b', n=2)
        self.assertEquals(a.to_string(), 'a,b\n3,4\n1,4\n')

    def test_min(self):
        a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
        a = a.minN(column='a', n=2)
        self.assertEquals(a.to_string(), 'a,b\n1,2\n1,4\n')


class TestRename(TestCase):
    def test_rename(self):
        a = Babe().pull(stream=StringIO('a,b\n1,2\n3,4\n1,4\n'), format="csv").typedetect()
        a = a.rename(a="c")
        self.assertEquals(a.to_string(), 'c,b\n1,2\n3,4\n1,4\n')


class TestWindowMap(TestCase):
    def test_windowMap(self):
        a = Babe().pull(stream=StringIO('a\n1\n2\n3\n4\n5\n6\n7\n'), format="csv").typedetect()
        a = a.windowMap(3, lambda rows: rows[-1]._make([sum([row.a for row in rows])]))
        self.assertEquals(a.to_string(),  'a\n1\n3\n6\n9\n12\n15\n18\n')



class TestDedup(TestCase):
    s = 'id,value,s\n1,coucou,4\n2,blabla,5\n3,coucou,6\n4,tutu,4\n'
    s2 = 'id,value,s\n1,coucou,4\n1,coucou,4\n3,coucou,6\n4,tutu,4\n'
    s3 = 'id,value,s\n1,coucou,4\n3,coucou,6\n4,tutu,4\n'
    s4 = 'id,value,s\n1,coucou,4\n2,blabla,5\n4,tutu,4\n'

    def test_dedup1(self):
        a = Babe().pull(stream=StringIO(self.s), format="csv")
        a = a.dedup()
        self.assertEquals(a.to_string(), self.s)

    def test_dedup2(self):
        a = Babe().pull(stream=StringIO(self.s2), format="csv")
        a = a.dedup()
        self.assertEquals(a.to_string(), self.s3)

    def test_dedup3(self):
        a = Babe().pull(stream=StringIO(self.s2), format="csv")
        a = a.dedup(fields=['id'])
        self.assertEquals(a.to_string(), self.s3)

    def test_dedup4(self):
        a = Babe().pull(stream=StringIO(self.s), format="csv")
        a = a.dedup(fields=['value'])
        self.assertEquals(a.to_string(), self.s4)


class TestNullValue(TestCase):
    s = "foo,bar\n1,2\n2,NULL\n"
    s2 = "foo,bar\n1,2\n2,\n"
    def test_null(self):
        a = Babe().pull(stream=StringIO(self.s), format='csv', null_value="NULL")
        buf = StringIO()
        a = a.push(stream=buf, format="csv")
        self.assertEquals(buf.getvalue(), self.s2)

class TestDate(TestCase):
    s = "foo,time\n1,2012-04-03 00:33\n"
    s2 = "foo,time,date,hour\n1,2012-04-02 22:33:00,2012-04-02,22\n"
    def test_parse(self):
        a = Babe().pull(stream=StringIO(self.s), format='csv')
        a = a.parse_time(field="time", output_time="time", output_date="date", output_hour="hour", input_timezone="CET", output_timezone="GMT")
        self.assertEquals(a.to_string(), self.s2)

class TestTee(TestCase):
    s = "a,b\n1,2\n"
    def test_tee(self):
        a = Babe().pull(stream=StringIO(self.s), format="csv")
        [b, c] = a.tee(2)
        self.assertEquals(len(b.to_list()), 1)
        self.assertEquals(len(c.to_list()), 1)

class TestPrimaryKey(TestCase):
    s = 'id,value,s\n1,coucou,4\n2,blabla,5\n3,coucou,6\n4,tutu,4\n'

    s2 = 'id,value,s\n1,coucou,4\n2,blabla,5\n3,coucou,6\n4,tutu,7\n'

    s3 = 'id,value,s\n1,coucou,4\n2,blabla,5\n3,coucou,6\n1,tutu,4\n'

    def test_primarykey(self):
        a = Babe().pull(stream=StringIO(self.s), format='csv')
        a = a.primary_key_detect()
        self.assertEquals(a.to_string(), self.s)

    def test_primarykey2(self):
        a = Babe().pull(stream=StringIO(self.s2), format='csv')
        a = a.primary_key_detect()
        self.assertEquals(a.to_string(), self.s2)

    def test_primarykey3(self):
        a = Babe().pull(stream=StringIO(self.s3), format='csv')
        a = a.primary_key_detect()
        self.assertEquals(a.to_string(), self.s3)

    def test_airport(self):
        a = Babe().pull(filename='data/airports.csv')
        a = a.primary_key_detect()
        a = a.head(n=10)
        a.to_string()
