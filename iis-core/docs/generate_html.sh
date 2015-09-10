#!/bin/bash

## This script generates HTML files from markdown files stored in this directory.
## "pandoc" program has to be installed in order for this script to work. This
## is available in the Ubuntu repository.

target_dir=../target/docs

mkdir -p $target_dir
for file in *.markdown
do
	filename="${file%.*}"
	pandoc -N -t html -s --no-wrap --css default.css --toc -o $target_dir/$filename.html $file
done

cp default.css $target_dir/default.css
