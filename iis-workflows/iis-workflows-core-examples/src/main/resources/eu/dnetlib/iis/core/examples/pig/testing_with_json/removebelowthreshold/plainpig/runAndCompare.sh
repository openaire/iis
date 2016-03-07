#!/bin/bash

# clear data from previous run
rm -rf *.log actualOutput

# execute pig script
pig -4 log4j.properties -param threshold=1.5 -x local remove_below_threshold.pig

# compare expected with actual output
diff --ignore-all-space ../data/output.json actualOutput/part-m-00000

