#!/bin/bash
# NOTE: this script is intended to be run with RDD 'pipe' method using bash;
# commands in this script are executed by spark within spark worker dir on each node;
# the dir should contain madis scripts in 'scripts' dir and projects db file as 'projects.db';

set -o pipefail

# $MADIS_HOME is exported in the docker image environment
python $MADIS_HOME/mexec.py -d projects.db -f scripts/taraextract.sql