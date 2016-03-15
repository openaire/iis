#!/usr/bin/env python

import re
import sys
import json
import argparse

def parse_params():
	parser = argparse.ArgumentParser(description="Clone input.")
	parser.add_argument("--copies", type=int, default=1, 
		help="Number of copies to create")
	args = parser.parse_args()
	return args

params = parse_params()

for line in sys.stdin:
	person = json.loads(line)
	for i in range(params.copies):
		print json.dumps(person)



