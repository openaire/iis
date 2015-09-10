#!/usr/bin/env python

import re
import sys
import json
import argparse

def parse_params():
	parser = argparse.ArgumentParser(description="Clone input.")
	parser.add_argument("--copies", type=int, default=3, 
		help="Number of copies to create")
	args = parser.parse_args()
	return args

params = parse_params()

for line_no, line in enumerate(sys.stdin):
#	print >>sys.stderr, "in line", line_no, " of loop:"
	person = json.loads(line)
	for i in range(params.copies):
		print json.dumps(person)
#		try:
#			print >>sys.stderr, line,
#			print line,
#		except Exception, ex:
#			raise Exception(ex, "in line {}".format(i+1))