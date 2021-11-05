#!/usr/bin/env python

import sys

projection = ["id", "age"]

# input comes from STDIN
for line in sys.stdin:
    # parse the input we got from mapper.py
    attribute, value = line.split('\t')

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: attribute) before it is passed to the reducer
    if attribute in projection:
        # write result to STDOUT
        print('%s\t%s' % (attribute, value))
