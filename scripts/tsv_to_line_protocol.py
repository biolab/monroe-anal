#!/usr/bin/env python3

import sys
import re


TABLE_NAME, TAGS, FIELDS = sys.argv[1:]

TAGS = [i for i in TAGS.split(',') if i]
FIELDS = [i for i in FIELDS.split(',') if i]

# Dumps are encoded in latin1
stdin = open(sys.stdin.fileno(), encoding='latin1')

def maybe_str(field, value,
              _CATEGORIES={'Operator', },
              _IS_NUMBER=re.compile('^-?\d+(\.\d+)?i?$').match):
    if field not in _CATEGORIES and _IS_NUMBER(value):
        return value
    return '"{}"'.format(value.replace('"', r'\"'))

for line in stdin:
    parts = line.split('\t')
    tags = [TABLE_NAME] + [name + '=' + value.replace(' ', r'\ ').replace(',', r'\,').replace(r'\\', '\\').replace('=', r'\=')
                           for name, value in zip(TAGS, parts)
                           if value]
    fields = [name + '=' + maybe_str(name, value)
              for name, value in zip(FIELDS, parts[len(TAGS):])
              if value]

    # Round to 10 ms precision and format in ms
    timestamp = int(round(float(parts[-1]), 2) * 1000)

    # Fields are required. If there were none, consider it an error
    # (e.g. in case of ping where RTT == null)
    fields = ','.join(fields) or 'Error=1i'
    print(','.join(tags), fields, timestamp)
