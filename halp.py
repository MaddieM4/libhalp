# A Python module implementation of HALP, because I'm a lot more familiar with
# it than C, where I was struggling with the language a lot more than the
# actual problem at hand.

import os.path
import re
import datetime
import time
import socket

def isvalidlabel(labelname):
	split = labelname.split('/')
	if len(split) == 0:
		return False
	for i in split[:-1]:
		if not i.replace("_","").isalnum():
			return False
	if not split[-1].isalnum() and split[-1]!='':
		return False
	return True

def to_text(address, seconds):
	assert(type(seconds)==int)
	return " ".join([str(seconds), address[0], str(address[1])])

def to_text_dt(address, mytime):
	return to_text(address, int(time.mktime(mytime.timetuple())))

class Cache:
	def __init__(self, filepath="~/.halp"):
		filepath = os.path.expanduser(filepath)
		if not os.path.isdir(filepath):
			raise ValueError("'%s' is not a directory" % filepath)
		self.path = filepath

	def labelpath(self, labelname):
		if not isvalidlabel(labelname):
			raise ValueError("'%s' is not a properly-formatted label" % labelname)
		return os.path.join(self.path, labelname.replace("/","."))

	def get(self, labelname):
		path = self.labelpath(labelname)
		return Label(path)

cacheaddress = re.compile("(\d+) (.+) (\d+)$")

class Label:
	def __init__(self, path, maxsize = None):
		self.path = path
		self.maxsize = maxsize
		self.contents = []
		self.addresses = set()
		self.load()

	def load(self):
		if os.path.exists(self.path):
			with open(self.path, 'r') as fileobj:
				self.loadf(fileobj)
			return True
		else:
			return False

	def loadf(self, fileobj):
		for line in fileobj:
			self.setfromtext(line)

	def reload(self):
		backup = self.contents
		self.trim(0)
		if not self.load():
			for i in backup:
				self.set((i[0],i[1]), i[2])
			return False
		return True

	def save(self, path=None):
		if path == None:
			path = self.path
		with open(path, 'w') as file:
			file.write(str(self))

	def setfromtext(self, text):
		match = cacheaddress.match(text)
		if match:
			t = datetime.datetime.utcfromtimestamp(
				float(match.group(1)) )
			address = match.group(2), int(match.group(3))
			self.set(address, t)
			return True
		else:
			return False

	def set(self, address, timestamp):
		assert(type(timestamp)==datetime.datetime)
		assert(type(address[0])==str)
		assert(type(address[1])==int)
		self.contents[self.alloc(address)] = (address[0],address[1], timestamp)
		self.addresses.add(address)
		if self.maxsize!=None:
			self.trim(self.maxsize)

	def get(self, address):
		return self[self.getid(address)]

	def getid(self, address):
		if address in self:
			index = 0
			for i in self.contents:
				if i[:2] == address:
					return index
				index += 1
			raise Exception("Sanity check failed")

	def remove(self, address):
		del self[self.getid(address)]

	def alloc(self, address):
		try:
			x = self.getid(address)
			if x == None:
				self.contents.append(None)
				return len(self.contents)-1
			else:
				return x
		except:
			return len(self.contents)

	def sort(self):
		self.contents.sort(key=lambda tup: tup[2])

	def trim(self, size):
		self.sort()
		for i in range(size, len(self)):
			del self[i]

	def __getitem__(self, index):
		return self.contents[index]

	def __getslice__(self, id1, id2):
		return self.contents[id1,id2]

	def __setitem__(self, index, value):
		self.contents[index] = value

	def __setslice__(self, id1, id2, value):
		self.contents[id1,id2] = value

	def __delitem__(self, index):
		address = self[index][:2]
		self.addresses.remove(address)
		del self.contents[i]

	def __contains__(self, address):
		return address in self.addresses

	def __str__(self):
		result = ""
		for i in self.contents:
			result += to_text_dt((i[0],i[1]), i[2]) +"\n"
		return result

	def __len__(self):
		return len(self.contents)

def talk(address, query):
	# A simple TCP exchange function for talking to HALP servers
	s = socket.create_connection(address)
	s.sendall(query)
	response = ""
	while 1:
		nr = s.recv(1024)
		if not nr:
			break
		else:
			response += nr
	s.close()
	return response

class Downloader:
	def __init__(self, path=None, following = []):
		if path==None:
			self.cache = Cache()
		else:
			self.cache = Cache(path)
		self.labels = {}
		self.load('halp')
		self['halp'].save()
		for i in following:
			self.load(i)

	def get(self, labelname):
		label = self[labelname]
		self.load(labelname)
		label.save()
		return str(label)

	def add(self, labelname):
		self.labels[labelname] = self.cache.get(labelname)

	def load(self, labelname, slice=None):
		label = self[labelname]
		query = "get "+labelname
		if slice!=None:
			query+="[%d:%d]" % slice
		response = None
		for i in self.labels['halp']:
			try:
				address = (i[0],i[1])
				response = talk(address, query)
				# bring first working serve to top of cache
				self.insertToCache('halp',address, 0)
				break
			except Exception:
				pass
		if response == None:
			return ""
		for line in response.split('\n'):
			label.setfromtext(line)
		return response

	def __getitem__(self, labelname):
		if not labelname in self.labels:
			self.add(labelname)
		return self.labels[labelname]

	def insertToCache(self, label, address, seconds):
		self[label].setfromtext(to_text(address, seconds))

	def insertToCache_dt(self, label, address, mytime):
		self[label].setfromtext(to_text_dt(address, mytime))
