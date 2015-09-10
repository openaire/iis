#!/usr/bin/env python

from __future__ import print_function

__author__ = "Mateusz Kobos mkobos@icm.edu.pl"

import sys
import argparse
import re
import xml.etree.ElementTree as ET

from oozie2dot.handlers.action import *
from oozie2dot.handlers.various import *

def parse():
    """Parse CLI arguments"""
    parser = argparse.ArgumentParser(
        description='Convert Oozie workflow description XML to '\
            'Graphviz *.dot file. Pass the XML file on stdin and the output '\
            'will be produced on stdout.')
    args = parser.parse_args()
    return args

parse()
xml_string = sys.stdin.read()
## We don't care about namespaces, so we remove them. Analyzing the XML with
## the namespace information is more difficult than without it.
xml_string_no_namespaces1= re.sub('xmlns="[^"]+"', '', xml_string)
xml_string_no_namespaces= re.sub("xmlns='[^']+'", "", xml_string_no_namespaces1)
root = ET.fromstring(xml_string_no_namespaces)
ignore_tags = ['parameters', 'global']
handlers_register = {'fork': handle_fork, 'decision': handle_decision, 
    'action': handle_action, 'join': handle_join, 'start': handle_start,
    'end': handle_end, 'kill': handle_end}

names_register = LabelsRegister()
print('digraph {')
#print('rankdir=LR;')
for child in root:
    tag_name = child.tag
    if tag_name in ignore_tags:
        continue
    if tag_name not in handlers_register:
        raise Exception('tag "{}" encountered which is neither '\
            'ignored nor handled'.format(tag_name))
    try:
        handlers_register[tag_name](child, names_register)
    except Exception, e:
        tag_string = ET.tostring(child, encoding='utf8', method='xml')
        raise Exception('Error while analyzing tag "{}": {}'.\
            format(tag_string, str(e)))
print('}')
