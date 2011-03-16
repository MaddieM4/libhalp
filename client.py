#!/usr/bin/python

import time
import datetime
import argparse
import sys

import halp


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers()

get = subparsers.add_parser("get", help="Load a list of addresses from a label")
insert = subparsers.add_parser("insert", help="Manually add an address to cache")

get.add_argument("label")

insert.add_argument("label")
insert.add_argument("hostname")
insert.add_argument("port", type=int)
insert.add_argument("-s", "--seconds", type=int, default=60)

def do_get(args):
	downloader = halp.Downloader(following=[args.label])
	downloader[args.label].save()
	print str(downloader[args.label])

def do_insert(args):
	cache = halp.Cache()
	label = cache.get(args.label)
	now = datetime.datetime.utcnow()
	mytimestamp = time.mktime(now.timetuple()) - args.seconds
	mytime = datetime.datetime.utcfromtimestamp(mytimestamp)
	label.set((args.hostname, args.port), mytime)
	label.save()

get.set_defaults(func=do_get)
insert.set_defaults(func=do_insert)

parsed = parser.parse_args(sys.argv[1:])
parsed.func(parsed)
