
import logging 

class PluginDict(object):
	def __init__(self, prefix): 
		self.prefix = prefix
		self.inerror = set()
		self.dict = dict()

	def module_names(self, key):
		m1 =  "%s%s" % (self.prefix, key)
		l = key.split("_")
		if len(l) > 1:
			return [m1, "%s%s" % (self.prefix, l[-1])]
		else:
			return [m1]

	def load_module(self, key):
		modules = self.module_names(key)
		ok = False
		for module in modules:
			if module in self.inerror:
				continue
			try: 
				__import__(module)
				ok = True
				break
				logging.info("Loaded plugin module %s" % module) 
			except ImportError:
				self.inerror.add(module)
		return ok

	def __setitem__(self, key, v): 
		if key is None:
			return 
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
		if key is None:
			return False
		if key in self.dict: 
			return True
		if self.load_module(key): 
			return key in self.dict
		else:
			return False 

if __name__ == "__main__": 
	d = PluginDict("pybabe.foobar")
	d['foo']
