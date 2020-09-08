class memoize:
	def __init__ (self, f):
		self.f = f
		self.cache = {}
	def __call__ (self, *args, **kwargs):
		if (args, str(kwargs)) in self.cache:
			return self.cache[args, str(kwargs)]
		else:
			tmp = self.f(*args, **kwargs)
			self.cache[args, str(kwargs)] = tmp
			return tmp
	def reset(self):
		self.cache={}

if __name__ == "__main__":
	@memoize
	def lala(a):
		return a+5

	lala(5)
	print lala.cache
	lala(15)
	print lala.cache
	lala(20)
	print lala.cache
	lala.reset()
	print lala.cache
	lala(5)
	print lala.cache

