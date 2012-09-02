
from pybabe import Babe
from unittest import TestCase, main
from cStringIO import StringIO



class TestPIVOT(TestCase): 
	def test_pivot(self):
		s = """a,b,c,d,e
A,B,0,2,3
C,D,1,4,5
A,B,1,4,5
C,E,1,4,5
C,E,0,7,8
""" 
		s2 = """a,b,d-0,e-0,d-1,e-1
A,B,2,3,4,5
C,D,,,4,5
C,E,7,8,4,5
"""
		s3= Babe().pull(stream=StringIO(s), format="csv").pivot(pivot="c", group=["a", "b"]).to_string()
		self.assertEquals(s3, s2)

	def test_unpivot(self):
		s = """name,firstname,Jan,Feb
Dou,Flo,3,5
Doe,John,7,8
"""
		s2 = """name,firstname,date,value
Dou,Flo,Jan,3
Dou,Flo,Feb,5
Doe,John,Jan,7
Doe,John,Feb,8
"""
		s3 = Babe().pull(stream=StringIO(s), format="csv").unpivot(common_fields=['name', 'firstname'], unpivot_name_field="date", unpivot_value_field="value").to_string()
		self.assertEquals(s3,s2)

if __name__ == "__main__":
	main()
