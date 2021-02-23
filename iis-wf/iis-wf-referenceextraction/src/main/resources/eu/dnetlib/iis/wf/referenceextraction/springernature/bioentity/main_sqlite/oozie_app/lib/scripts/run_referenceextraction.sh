#!/bin/bash
# NOTE: this script is intended to be run with RDD 'pipe' method using bash;
# commands in this script are executed by spark within spark worker dir on each node;
# the dir should contain scripts in 'scripts' dir and db file;

DB_FILE="$1"
SQL_SCRIPT="$2"

set -o pipefail
python scripts/madis/mexec.py -d "$DB_FILE" -f scripts/"$SQL_SCRIPT"