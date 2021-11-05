#!/usr/bin/env python
"""mapper.py"""

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    attributes_values = line.split(', ')
    for attribute_value in attributes_values:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        attribute, value = attribute_value.split(': ')
        print('%s\t%s' % (attribute, value))
