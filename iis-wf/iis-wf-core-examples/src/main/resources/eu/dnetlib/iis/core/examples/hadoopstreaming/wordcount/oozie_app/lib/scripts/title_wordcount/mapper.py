#!/usr/bin/env python

import sys
import json

for line in sys.stdin:
	document = json.loads(line)
	title = document["title"]
	## Ignore title field if it is Null
	if title is None:
		continue
	for word in document["title"].split():
		## Key and value have to be separated with the tab character
		print word.lower()+"\t"+str(1)