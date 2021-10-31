import json
import os
import numpy as np
import random
import string
import time

# TODO - group by for column storage
def col_group_by(file_name, group_by_list, apply_on_list, operation = 'sum'):
    start = time.time()
    file_path = os.path.join(os.getcwd(), 'disk_storage_column\\ ' + file_name)
    with open(file_path, 'r') as json_file:
        table = json.load(json_file)

    flattened = [u for subitem in [group_by_list, apply_on_list] for u in subitem]
    iters = []

    while len(iters) < len(flattened):
        ran = random.choice(string.ascii_letters)
        if ran not in iters:
            iters.append(ran)

    grouped_json = {}

    for iters in zip(*[table[u] for u in flattened]):
        if iters[0] not in grouped_json:
            grouped_json[iters[0]] = np.zeros(len(apply_on_list) + 1)
        for i in range(len(apply_on_list)):
            grouped_json[iters[0]][i] += float(iters[i+1])
            grouped_json[iters[0]][-1] += 1

    if operation == 'mean':
        for k in grouped_json.keys():
            for i in range(len(apply_on_list)):
                grouped_json[k][i] = grouped_json[k][i] / grouped_json[k][-1]

    duration = time.time() - start
    print(f'Time spent for group by on columns: {duration}')
    print(grouped_json)

    '''grouped_df = pd.DataFrame(
        list(zip(list(grouped_json.keys()), list(grouped_json.values()))),
        columns=flattened)
    print(grouped_df)'''

# TODO - group by for row storage


def row_group_by(file_name, group_by_list, apply_on_list, operation = 'sum'):
    start = time.time()
    flattened = [u for subitem in [group_by_list, apply_on_list] for u in subitem]

    file_path = os.path.join(os.getcwd(), 'disk_storage_row\\ ' + file_name)
    with open(file_path, 'r') as json_file:
        table = json.load(json_file)

    iters = []
    while len(iters) < len(flattened):
        ran = random.choice(string.ascii_letters)
        if ran not in iters:
            iters.append(ran)

    grouped_json = {}
    for row in table['rows']:
        if row[flattened[0]] not in grouped_json:
            grouped_json[row[flattened[0]]] = np.zeros(len(apply_on_list))
        for i in range(len(apply_on_list)):
            grouped_json[row[flattened[0]]][i] += float(row[flattened[i+1]])

    duration = time.time() - start
    print(f'Time spend for group by on row: {duration}')
    print(grouped_json)


col_group_by('LINEITEM_column.txt', ['suppkey'], ['quantity', 'discount'], operation='mean')
#row_group_by('LINEITEM_row.txt', ['suppkey'], ['quantity', 'discount'])


