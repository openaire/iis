#!/bin/bash
set -o pipefail

# $MADIS_HOME is exported in the docker image environment
python $MADIS_HOME/mexec.py -f $1/covid19extract.sql