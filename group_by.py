import json
import os
import pandas as pd
import numpy as np
import random
import string

def col_group_by(file_name, group_by_list, apply_on_list, operation = 'sum'):
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
            grouped_json[iters[0]] = np.zeros(len(apply_on_list))
        for i in range(len(apply_on_list)):
            grouped_json[iters[0]][i] += float(iters[i+1])


    print(grouped_json)
    '''grouped_df = pd.DataFrame(
        list(zip(list(grouped_json.keys()), list(grouped_json.values()))),
        columns=flattened)
    print(grouped_df)'''

col_group_by('LINEITEM_column.txt', ['suppkey'], ['quantity', 'discount'])


