#!/bin/bash

set -e

scripts="$(dirname "$0")"
recipes="$scripts/recipes"

# Convert archived CSV dumps from ./ into InfluxDB line protocol files (in ./).
# Many uncompressed files of 100k lines are created.
# FIXME: ensure enough disk space
echo 'Converting dumps ...'

for table in "$recipes"/*; do
    table=$(basename "$table")

    { read columns;
      read -r tags fields; } < "$recipes/$table"

    echo "    $table" >&2

    for file in *"$table"*.csv.txz; do
        [ ! -f "$file" ] && continue

        echo "        $file" >&2

        tar xJOf $file |
            csvtool -u TAB namedcol $columns - |
            tail -n +2 |
            sed -E 's/\b(None|null)\b//g' |
            "$scripts/tsv_to_line_protocol.py" $table $tags $fields
    done
done |
    split -a 5 -d -l 100000 - "influxdb_lines_"


# Import line files into DB
#
# Several jobs simultaneously to liken threads. influx CLI tool imports
# some 1000 points per second, so if we spawn multiple invocations of it,
# we can speed up the importation considerably.
# Any import errors are written, along with context lines, into ./notice.
echo
echo 'Importing data into database ...'

N_JOBS=100

# Print DB import errors after completion
trap 'grep error notice && echo "See ./notice for errors"' EXIT

for file in ./influxdb_lines_*; do
    influx -import -precision ms -path <(
        echo '#DDL
CREATE DATABASE monroe
# DML
# CONTEXT-DATABASE: monroe';
        cat "$file"; rm "$file";) >> notice 2>&1 &

    if [ $(jobs -lr | wc -l) -ge $N_JOBS ]; then
        wait -n
    fi
done

wait
