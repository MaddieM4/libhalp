#!/usr/bin/python

import datetime
import argparse
import sys

import halp


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers()

get = subparsers.add_parser("get", help="Load a list of addresses from a label")
insert = subparsers.add_parser("insert", help="Manually add an address to cache")
bcast = subparsers.add_parser("bcast", help="Send an insertion to HALP servers")
clear = subparsers.add_parser("clear", help="Delete cached data")
nuke = subparsers.add_parser("nuke", help="Delete all cached data")

get.add_argument("label")

insert.add_argument("label")
insert.add_argument("hostname")
insert.add_argument("port", type=int)
insert.add_argument("-s", "--seconds", type=int, default=60)

bcast.add_argument("label")
bcast.add_argument("hostname")
bcast.add_argument("port", type=int)
bcast.add_argument("-s", "--seconds", type=int, default=60)
bcast.add_argument("-n", "--number", type=int, default=5)

clear.add_argument("label")

def do_get(args):
	downloader = halp.Downloader()
	print downloader.get(args.label)

def do_insert(args):
	cache = halp.Cache()
	label = cache.get(args.label)
	mytimestamp = halp.posixnow() - args.seconds
	mytime = datetime.datetime.utcfromtimestamp(mytimestamp)
	label.set((args.hostname, args.port), mytime)
	label.save()
	print "'%s %d' inserted to local cache under label '%s'" % (args.hostname, args.port, args.label)
	print "Timestamp", mytimestamp

def do_bcast(args):
	downloader = halp.Downloader()
	print downloader.bcast_insert(args.label, args.hostname, args.port,
		args.number, halp.posixnow()-args.seconds)

def do_clear(args):
	halp.Cache().clear(args.label)
	print "Contents of label '%s' deleted." % args.label

def do_nuke(args):
	halp.Cache().clear_all()
	print """All cache files deleted (does not include .allocation 
config or containing folder)"""

get.set_defaults(func=do_get)
insert.set_defaults(func=do_insert)
bcast.set_defaults(func=do_bcast)
clear.set_defaults(func=do_clear)
nuke.set_defaults(func=do_nuke)

parsed = parser.parse_args(sys.argv[1:])
parsed.func(parsed)
