#!/bin/bash
set -o pipefail
scripts/madis/mexec.py -d scripts/taxonomies.db -f scripts/classify.sql | scripts/complete_taxonomies.py
