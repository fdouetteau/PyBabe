
import logging 

class PluginDict(object):
	def __init__(self, prefix): 
		self.prefix = prefix
		self.inerror = set()
		self.dict = dict()

	def module_name(self, key):
		return "%s%s" % (self.prefix, key)

	def load_module(self, key):
		module = self.module_name(key)
		if module in self.inerror:
			return False
		try: 
			__import__(module)
			logging.info("Loaded plugin module %s" % module) 
		except ImportError:
			self.inerror.add(module)
			return False
		return True 

	def __setitem__(self, key, v): 
		self.dict.__setitem__(key, v)

	def __getitem__(self, key, v=None):
		if key is None:
			return None
		if key in self:
			return self.dict.__getitem__(key)
		if self.load_module(key): 
			return self.dict.__getitem__(key)
		else:
			raise AttributeError 

	def __contains__(self, key):
		if key in self.dict: 
			return True
		if self.load_module(key): 
			return key in self.dict
		else:
			return False 

if __name__ == "__main__": 
	d = PluginDict("pybabe.foobar")
	d['foo']
