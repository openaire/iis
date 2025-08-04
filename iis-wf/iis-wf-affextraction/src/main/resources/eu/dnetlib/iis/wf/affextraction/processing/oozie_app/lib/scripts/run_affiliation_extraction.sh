#!/bin/bash
set -o pipefail

# this is just a simple pass-through logic to be replaced with the real code.

# This script reads JSON records from stdin (with `id`, `doi` and `text` triples) and transforms them into an AffiliationExtractionResult format, printing the result to stdout.
#
# Example usage:
# cat records.json | ./run_affro.sh

# jq is not available on data nodes - disabling, sticking to a slightly more cumbersome awk-based version
# jq -c '{doi: .doi, id: .id, parsing_output: {doi: .doi, authors: [], success: true, reason_of_failure: ""}}'

awk '
# Rule 1: Skip empty lines.
NF == 0 { next }

# Rule 2: Process non-empty lines.
{
    # Extract "id". The pattern now allows for optional spaces after the colon.
    match($0, /"id"[[:space:]]*:[[:space:]]*"([^"]*)"/, id_arr)
    id = id_arr[1]

    # Extract "doi". This pattern also allows for optional spaces and handles
    # both the literal "null" and a quoted string value.
    match($0, /"doi"[[:space:]]*:[[:space:]]*(null|"[^"]*")/, doi_arr)
    doi = doi_arr[1]

    # If the value was a quoted string (e.g., "someDoi1"), remove the quotes.
    # If it was null, this does nothing.
    gsub(/"/, "", doi)

    # Construct and print the output JSON string.
    printf "{\"doi\":\"%s\",\"id\":\"%s\",\"parsing_output\":{\"doi\":\"%s\",\"authors\":[],\"success\":true,\"reason_of_failure\":\"\"}}\n", doi, id, doi
}
'