#!/usr/bin/python

import halp

server = halp.Server()
try:
	server.start()
except KeyboardInterrupt:
	server.close()
