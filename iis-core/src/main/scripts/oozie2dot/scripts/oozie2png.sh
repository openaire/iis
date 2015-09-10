#!/bin/bash

## Convert Oozie XML workflow file given on stdin to
## *.png file on stdout representing the workflow.
##
## Graphviz's 'dot' program has to be installed in the system.

$(dirname $0)/../oozie2dot.py | dot -Tpng
