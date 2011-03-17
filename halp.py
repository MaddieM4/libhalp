# A Python module implementation of HALP, because I'm a lot more familiar with
# it than C, where I was struggling with the language a lot more than the
# actual problem at hand.

import os
import os.path
import re
import datetime
import time
import socket
import threading
from random import random

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

def posixtime(mytime):
	return int(time.mktime(mytime.timetuple()))

def posixnow():
	return posixtime(datetime.datetime.utcnow())

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
		return Label(path, self.is_sub(labelname))

	def is_sub(self, labelname):
		return labelname == "labels" or labelname[-1:] == "/"

	def clear(self, labelname):
		os.remove(self.labelpath(labelname))

	def clear_all(self):
		for i in os.listdir(self.path):
			os.remove(os.path.join(self.path,i))

cacheaddress = re.compile("(\d+) (.+) (\d+)$")

class Label:
	''' A class that models a label in the cache. Not thread-safe
	individually, but if you use a Downloader for access you'll be safe.'''
	def __init__(self, path, sub, maxsize = None):
		self.path = path
		self.sub = sub
		self.maxsize = maxsize
		self.contents = []
		self.addresses = set()
		self.load()
		self.sort()

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
				self.set(self.addr(i),self.time(i))
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
			if self.sub:
				address = match.group(2)
			else:
				address = match.group(2), int(match.group(3))
			self.set(address, t)
			return True
		else:
			return False

	def set(self, address, timestamp):
		assert(type(timestamp)==datetime.datetime)
		if self.sub:
			assert(type(address)==str)
		else:
			assert(type(address[0])==str)
			assert(type(address[1])==int)
		a = self.alloc(address)
		if self[a] != None:
				timestamp = max(self.time(self[a]), timestamp)
		self[a] = self.nentry(address, timestamp)
		self.addresses.add(address)
		if self.maxsize!=None:
			self.trim(self.maxsize)

	def get(self, address):
		return self[self.getid(address)]

	def getid(self, address):
		if address in self:
			index = 0
			for i in self.contents:
				if self.addr(i) == address:
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
		self.contents.sort(key=lambda tup: self.time(tup), reverse=True)

	def trim(self, size):
		self.sort()
		for i in self.contents[size:]:
			self.addresses.remove(self.addr(i))
		del self.contents[size:]

	def clear(self):
		os.remove(self.path)

	def addr(self, tup):
		if self.sub:
			return tup[0]
		else:
			return tup[:2]

	def time(self, tup):
		if self.sub:
			return tup[1]
		else:
			return tup[2]

	def nentry(self, address, mytime):
		if self.sub:
			return address, mytime
		else:
			return address[0], address[1], mytime

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
		del self.contents[index]

	def __contains__(self, address):
		return address in self.addresses

	def __str__(self):
		result = ""
		for i in self.contents:
			result += to_text_dt((i[0],i[1]), i[2]) +"\n"
		return result[:-1]

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
	''' A thread-safe updatable interface to the cache '''
	def __init__(self, path=None, following = []):
		self.lock = threading.RLock()
		with self.lock:
			if path==None:
				self.cache = Cache()
			else:
				self.cache = Cache(path)
			self.labels = {}
			self.get('halp')
			for i in following:
				self.load(i)

	def get(self, labelname):
		with self.lock:
			label = self[labelname]
			self.load(labelname)
			label.save()
			return str(label)

	def add(self, labelname):
		with self.lock:
			self.labels[labelname] = self.cache.get(labelname)

	def remove(self, labelname):
		with self.lock:
			del self.labels[labelname]

	def load_cached(self, labelname):
		with self.lock:
			return str(self[labelname])

	def load(self, labelname, slice=None):
		with self.lock:
			label = self[labelname]
			query = "get "+labelname
			if slice!=None:
				query+="[%d:%d]" % slice
			response = None
			address = None
			for i in self.labels['halp']:
				try:
					address = (i[0],i[1])
					response = talk(address, query)
					break
				except Exception:
					pass
			if response == None:
				return ""
			serverGood = False
			for line in response.split('\n'):
				serverGood = True
				label.setfromtext(line)
			if serverGood:
				# bring first working server to top of cache
				self.insertToCache('halp',address, 0)
			return response

	def bcast_insert(self, label, hostname, port, n=5, timestamp=posixnow()):
		query = "insert "+label+"\n"+" ".join(
			[str(timestamp), hostname, str(port)])
		return self.bcast(query, n)

	def bcast(self, query, n):
		assert(type(query)==str)
		assert(type(n)==int)
		message = ""
		t = 0
		for i in self['halp']:
			# Broadcast down the list until t=n or end of list
			if t >= n:
				break
			try:
				address = i[:2]
				response = talk(address, query)
				message += "Server %s responded:\n\t%s\n" %(
					str(address), 
					response.replace("\n","\n\t"))
				t += 1
			except Exception:
				pass
		message += "%d/%d messages sent successfully" % (t, n)
		return message

	def clear(self, labelname):
		with self.lock:
			self.cache.clear(labelname)

	def clear_all(self):
		with self.lock:
			self.cache.clear()

	def __getitem__(self, labelname):
		with self.lock:
			if not labelname in self.labels:
				self.add(labelname)
			else:
				self.labels[labelname].reload()
			return self.labels[labelname]

	def insertToCache(self, label, address, seconds):
		with self.lock:
			self[label].setfromtext(to_text(address, seconds))

	def insertToCache_dt(self, label, address, mytime):
		with self.lock:
			self[label].setfromtext(to_text_dt(address, mytime))

	def close(self):
		with self.lock:
			all_labels = [i for i in self.labels]
			for i in all_labels:
				self.remove(i)

class Updater(threading.Thread):
	''' A threaded label updater. '''
	def __init__(self, dl, label, frequency=200):
		threading.Thread.__init__(self)
		self.dl = dl
		self.tlock = threading.Lock()
		self.label = label
		self.frequency = frequency
		self.timer = None
		self.cancelled = False

	def update(self):
		with self.dl.lock:
			self.dl.load(self.label)
			self.dl[self.label].save()
		self.tlock.release()

	def run(self):
		while 1:
			# wait for timer to complete
			self.tlock.acquire()
			if not self.cancelled:
				self.timer = threading.Timer(self.frequency, self.update)
			else:
				break

	def cancel(self):
		self.cancelled = True
		if self.tlock.locked():
			self.tlock.release()

class AutoDownloader(Downloader):
	''' This, by itself, does everything you need for a self-updated server
	except provide an interface for others to connect to you. It does,
	however, give each label updater a different frequency within a range
	you define, so that you don't get everything trying to update at once.
	'''
	def __init__(self, path=None, following=[], frequency_min=200, frequency_max=300):
		self.updaters = {}
		self.freq_min = frequency_min
		self.freq_max = frequency_max
		Downloader.__init__(self, path, following)

	def add(self, labelname):
		with self.lock:
			Downloader.add(self,labelname)
			self.updaters[labelname] = Updater(self,
							labelname,
							self.frequency())

	def remove(self, labelname):
		with self.lock:
			Downloader.remove(self,labelname)
			self.updaters[labelname].cancel()
			del self.updaters[labelname]

	def frequency(self):
		return self.freq_min + random()*(self.freq_max-self.freq_min)

	def close(self):
		with self.lock:
			Downloader.close(self)
			for i in self.updaters:
				self.updaters[i].cancel()
				del self.updaters[i]

get = re.compile("get (\w+)$")
getslice = re.compile("get (\w+)\[(\d+):(\d+)\]$")
insert = re.compile("insert(( [a-z0-9_\.]+)*)\n")

class Server:
	''' A fully featured HALP server. '''
	def __init__(self, port = 3451, automatic=True):
		if automatic:
			self.dl = AutoDownloader()
		else:
			self.dl = Downloader()
		self.port = port
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	def start(self):
		self.socket.bind(('',self.port))
		self.socket.listen(5)
		print "Now serving on port %d." % self.port
		while 1:
			client, caddress = self.socket.accept()
			print "Connection:",caddress
			query = client.recv(4096)
			print "Query:\n\t",query.replace("\n","\n\t")
			response = self.parse(query)
			print "Sending:\n\t", response.replace("\n","\n\t"),"\n"
			client.sendall(response)
			client.close()

	def close(self):
		self.socket.close()
		self.dl.close()

	def parse(self, query):
		if get.match(query):
			# 1 - label
			match = get.match(query)
			return self.do_get(match.group(1))
		elif getslice.match(query):
			# 1 - label, 2 - slice
			match = get.match(query)
			slice = tuple(match.group(2).split(":"))
			return self.do_get(match.group(1), slice=slice)
		elif insert.match(query):
			# 1 - label list
			labels = insert.match(query).groups(1)[0].split()
			entries = query.split("\n")[1:]
			return self.do_insert(labels, entries)

	def do_get(self, label, index=None, slice=(0,10)):
		# return str(halp.posixnow())+" localhost 3452"
		addrlist = self.dl.load_cached(label).split("\n")
		if index == None:
			return "\n".join(addrlist[slice[0]:slice[1]])
		else:
			return addrlist[index]

	def do_insert(self, labels, entries):
		message = ""
		for l in labels:
			label = self.dl[l]
			for e in entries:
				ml = l +" "+ e
				if label.setfromtext(e):
					message += "success: "+ml
				else:
					message += "failure: "+ml
			label.save()
		return message
