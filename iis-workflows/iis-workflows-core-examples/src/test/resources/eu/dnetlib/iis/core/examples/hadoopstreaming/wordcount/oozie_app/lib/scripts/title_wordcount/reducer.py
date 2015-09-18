#!/usr/bin/env python

import sys
import json

## Streaming guarantees that the lines read by the reducer are sorted by the 
## key. However, it is the responsibility of the developer of the reducer to 
## find the boundary where lines with a certain key end and lines with a next 
## key start, hence our use of the "last_key" variable.

last_key = None
count = 0

def produce_object(key, count):
	print '{"word":"'+key+'","count":'+str(count)+'}'

for line in sys.stdin:
	elems = line.split("\t")
	if len(elems) != 2:
		raise Exception("There should be a single tab character separating "\
			"key from value in the line, but found "+str(len(elems)-1))
	key, value = elems
	if last_key is not None and last_key != key:
		produce_object(last_key, count)
		count = int(value)
	else:
		count = count + int(value)
	last_key = key
if last_key is not None:
	produce_object(last_key, count)