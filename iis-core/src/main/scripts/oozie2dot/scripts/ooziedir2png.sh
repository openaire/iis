#!/bin/bash

## Convert all "workflow.xml" files in given directory tree to
## corresponding diagrams in *.png format.
##
## Graphviz's 'dot' program has to be installed in the system.
##
## Parameters are described below.

## Path to the workflow root directory
WORKFLOW_DIR=$1
## Optional path to the "oozie2png.sh" script file. By default, we assume
## that it is placed in the same directory as this file.
SCRIPT_PATH=${2-$(dirname $0)/oozie2png.sh} 

shopt -s globstar ## Enable recursive globs

FILES=$(find $WORKFLOW_DIR -name "workflow.xml")
for f in **/workflow.xml; do
	echo Processing $f ... 
	$SCRIPT_PATH < $f > $f.png
done
