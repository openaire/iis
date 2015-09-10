#!/usr/bin/env python

import sys
import json
from itertools import groupby
import sqlite3
import argparse

def parse_params():
	parser = argparse.ArgumentParser(description="Wordcount input.")
	parser.add_argument("--input_stopwords", type=str, help="Path to stopwords SQLite database.")
	args = parser.parse_args()
	return args

params = parse_params()


def isStopWord(cursor, word):
    t = (word,)
    cursor.execute('SELECT * FROM stopwords WHERE word=?', t)
    return cursor.fetchone()

conn = sqlite3.connect(params.input_stopwords)
cursor = conn.cursor()


titlewords = []
for line in sys.stdin:
    document = json.loads(line)
    if document["title"]:
        words = document["title"].split()
        titlewords.extend(words)

titlewords = [titleword.lower() for titleword in titlewords]
titlewords = [titleword for titleword in titlewords if not isStopWord(cursor, titleword)]

wordmap = {key: len(list(group)) for key, group in groupby(sorted(titlewords))}
wordcounts = [{'word': key, 'count': value} for key, value in wordmap.iteritems()]

for wordcount in wordcounts:
    print json.dumps(wordcount)
