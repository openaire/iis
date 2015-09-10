#!/usr/bin/env python

## Generates a new version of the "generate_example_workflow_apps.properties" 
## file that contains paths to all of the example workflows stored in this
## project. This is done by scanning the directory tree and searching for
## directories that look like they contain workflow definitions.

from __future__ import print_function

import os
import os.path

dir_with_examples = "src/test/resources/eu/dnetlib/iis/core/examples"
dirs_to_ignore = [".svn"]
output_file = "src/main/scripts/generate_example_workflow_apps.properties"

def does_contain_example(dir_path):
	if os.path.exists(os.path.join(dir_path, "oozie_app")):
		return True
	else:
		return False

examples = []

for root, dirs, files in os.walk(dir_with_examples):
	for dir_to_ignore in dirs_to_ignore:
		dirs.remove(dir_to_ignore)
	dirs_to_remove = []
	for dir_ in dirs:
		dir_path = os.path.join(root, dir_)
		if does_contain_example(dir_path):
			examples.append(dir_path)
			dirs_to_remove.append(dir_)
	for dir_to_remove in dirs_to_remove:
		dirs.remove(dir_to_remove)

examples = sorted(examples)
with open(output_file, "w") as f:
	for e in examples:
		print(e, file=f)
	print("# remember to leave '\\n' after the last line\n", file=f)
