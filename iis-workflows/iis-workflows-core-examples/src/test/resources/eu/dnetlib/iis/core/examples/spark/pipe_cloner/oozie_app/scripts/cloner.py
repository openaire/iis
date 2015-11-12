#!/usr/bin/env python

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
	obj = json.loads(line)
	for i in range(params.copies):
		print json.dumps(obj)