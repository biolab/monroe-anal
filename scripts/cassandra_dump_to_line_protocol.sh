#!/bin/bash

set -e

python3 -c 'import monroe_anal.db' >/dev/null 2>&1 || {
    echo 'ERROR: Install python package first. pip install monroe_anal'
    exit 1
}
command -v csvtool >/dev/null || {
    echo 'ERROR: sudo apt install csvtool'
    exit 1
}

scripts="$(readlink -f "$(dirname "$0")")"
recipes="$scripts/recipes"
tables="$(ls "$recipes")"

# Convert archived CSV dumps from ./ into InfluxDB line protocol files (in ./).
# Many uncompressed files of 100k lines are created.
# FIXME: ensure enough disk space
echo 'Converting dumps ...'

for table in $tables; do
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
            "$scripts/tsv_to_line_protocol.py" ${table}_staged $tags $fields
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
trap 'grep error notice && echo -e "\nSee ./notice for import stats & errors"' EXIT

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


_first_time() {
    influx -precision rfc3339 -database monroe -format csv -execute "SELECT * FROM ${1}_staged ORDER BY time $2 LIMIT 1" |
        csvtool namedcol time - |
        tail -n +2
}

# Make distinct level-of-detail tables for faster retrieval
echo 'Making LOD tables ...'
for table in $tables; do
    agg_columns="$(python3 -c "from monroe_anal.db import $table; print($table._select_agg())")"
    start_time="$(_first_time $table ASC)"
    end_time="$(_first_time $table DESC)"
    echo "    $table"
    for time_bin in '10ms' '1s' '1m' '30m'; do
        if [ "$time_bin" = '10ms' ]; then
            groupby='*'  # Just group by tags (i.e. copy)
        else
            groupby="time($time_bin),*"
        fi
        echo "        $time_bin  $(
            influx -database monroe -format csv \
            -execute "SELECT $agg_columns \
                      INTO ${table}_${time_bin} \
                      FROM ${table}_staged \
                      WHERE time >= '$start_time' \
                      AND time <= '$end_time' \
                      GROUP BY $groupby" | csvtool namedcol written - | tail -n +2)" &
    done
    wait
    # Drop staging table
    influx -database monroe -execute "DROP MEASUREMENT ${table}_staged"
done

wait
