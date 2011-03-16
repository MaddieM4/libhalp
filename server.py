#!/usr/bin/python

import halp
import socket
import re

dl = halp.Downloader()
get = re.compile("get (\w+)$")
getslice = re.compile("get (\w+)\[(\d+):(\d+)\]$")

def parse(query):
	if get.match(query):
		# 1 - label
		match = get.match(query)
		return do_get(match.group(1))
	elif getslice.match(query):
		# 1 - label, 2 - slice
		match = get.match(query)
		slice = tuple(match.group(2).split(":"))
		return do_get(match.group(1), slice=slice)

def do_get(label, index=None, slice=(0,10)):
	return "bluberoo"
	addrlist = dl.get(label).split("\n")
	if index == None:
		return "\n".join(addrlist[slice[0]:slice[1]])
	else:
		return addrlist[index]

gate = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
gate.bind(('',3451))
gate.listen(5)
print "Now serving."
while 1:
	client, caddress = gate.accept()
	print "Connection:",caddress
	query = client.recv(4096)
	client.sendall(parse(query))
	client.close()
