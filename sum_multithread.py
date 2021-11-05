import json
import os
import numpy as np
import random
import string
import time
import concurrent.futures


def col_sum(file_name, cols_to_sum):
    start = time.time()
    file_path = os.path.join(os.getcwd(), 'disk_storage_column\\ ' + file_name)
    with open(file_path, 'r') as json_file:
        table = json.load(json_file)

    summed_json = {col: 0 for col in cols_to_sum}
    def first_loop(args):
        for col in args:
            for v in table[col]:
                summed_json[col] += float(v)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.submit(first_loop,cols_to_sum)
    '''
    for col in cols_to_sum:
        for v in table[col]:
            summed_json[col] += float(v)
    '''
    duration = time.time() - start
    print(f'Time spent for summing on columns storage: {duration}')
    print(summed_json)


def row_sum(file_name, cols_to_sum):
    start = time.time()
    file_path = os.path.join(os.getcwd(), 'disk_storage_row\\ ' + file_name)
    with open(file_path, 'r') as json_file:
        table = json.load(json_file)

    summed_json = {col: 0 for col in cols_to_sum}
    def first_loop(args):
        for col in args:
            for row in table['rows']:
                summed_json[col] += float(row[col])
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.submit(first_loop,cols_to_sum)
    '''
    for col in cols_to_sum:
        for row in table['rows']:
            summed_json[col] += float(row[col])
    '''
    duration = time.time() - start
    print(f'Time spent for summing on rows storage: {duration}')
    print(summed_json)


col_sum('LINEITEM_column.txt', ['discount', 'quantity'])
row_sum('LINEITEM_row.txt', ['discount', 'quantity'])

