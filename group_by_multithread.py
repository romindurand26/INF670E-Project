import json
import os
import numpy as np
import random
import string
import time
import pandas as pd
import concurrent.futures
import plotly.graph_objects as go

if not os.path.exists("group_by_figures"):
    os.mkdir("group_by_figures")

# ________ATTENTION: group_by (ASC, DESC) working only for group by 1 column and apply
#           operation (sum, mean) on 1 columns

# --------------- GROUPING COL STORAGE DISK --------------------------


def order(grouped_json, how, flattened):
    if how == 'ASC':
        unordered = [u for sublist in list(grouped_json.values()) for u in sublist]
        unordered = [unordered[i] for i in range(len(unordered)) if i % 2 == 0]
        grouped_json = dict(zip(list(grouped_json.keys()), unordered))
        new_json = list(grouped_json.items())
        def first_loop(args):
            for i in range(args):
                for j in range(args - 1):
                    if new_json[j][1] > new_json[j+1][1]:
                        swapper = new_json[j]
                        new_json[j] = new_json[j+1]
                        new_json[j+1] = swapper
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(first_loop,len(new_json))

        '''
        for i in range(len(new_json)):
            for j in range(len(new_json) - 1):
                if new_json[j][1] > new_json[j+1][1]:
                    swapper = new_json[j]
                    new_json[j] = new_json[j+1]
                    new_json[j+1] = swapper
        '''

        grouped_df = pd.DataFrame(new_json, columns=flattened)

    elif how == 'DESC':
        unordered = [u for sublist in list(grouped_json.values()) for u in sublist]
        unordered = [unordered[i] for i in range(len(unordered)) if i % 2 == 0]
        grouped_json = dict(zip(list(grouped_json.keys()), unordered))
        new_json = list(grouped_json.items())
        def second_loop(args):
            for i in range(args):
                for j in range(args - 1):
                    if new_json[j][1] < new_json[j + 1][1]:
                        swapper = new_json[j]
                        new_json[j] = new_json[j + 1]
                        new_json[j + 1] = swapper
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(second_loop,len(new_json))
        '''
        for i in range(len(new_json)):
            for j in range(len(new_json) - 1):
                if new_json[j][1] < new_json[j + 1][1]:
                    swapper = new_json[j]
                    new_json[j] = new_json[j + 1]
                    new_json[j + 1] = swapper
        '''

        grouped_df = pd.DataFrame(new_json, columns=flattened)

    else:
        grouped_df = pd.DataFrame(
            list(zip(list(grouped_json.keys()), list(grouped_json.values()))),
            columns=[flattened[0], 'apply_on']
        )
        flattened.append('count')
        grouped_df[flattened[1:]] = pd.DataFrame(grouped_df.apply_on.tolist(), index=grouped_df.index)
        grouped_df = grouped_df.drop(columns=['apply_on'], axis=1)

    print(grouped_df)

    return grouped_df


def go_plot(grouped_df, group_by_list, apply_on_list, how):
    fig = go.Figure(data=[go.Table(
        header=dict(values=list(grouped_df.columns),
                    fill_color='paleturquoise',
                    align='left'),
        cells=dict(values=[grouped_df[col] for col in grouped_df.columns],
                   fill_color='lavender',
                   align='left'))
    ])
    fig.update_layout(title_text=f"Group by {group_by_list} select {apply_on_list} {how}", title_x=0.5)
    fig.write_image(f"group_by_figures/{group_by_list}_{apply_on_list}_{how}.png")


def col_group_by(file_name, group_by_list, apply_on_list, operation='sum', how=None):
    start = time.time()
    file_path = os.path.join(os.getcwd(), 'disk_storage_column\\ ' + file_name)
    with open(file_path, 'r') as json_file:
        table = json.load(json_file)

    flattened = [u for subitem in [group_by_list, apply_on_list] for u in subitem]
    iters = []
    def first_loop(args,args1):
        while len(args) < len(args1):
            ran = random.choice(string.ascii_letters)
            if ran not in iters:
                iters.append(ran)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.submit(first_loop,iters,flattened)

    '''
    while len(iters) < len(flattened):
        ran = random.choice(string.ascii_letters)
        if ran not in iters:
            iters.append(ran)
    '''

    grouped_json = {}
    def second_loop(args,args1):
        for iters in args:
            if iters[0] not in grouped_json:
                grouped_json[iters[0]] = np.zeros(len(apply_on_list) + 1)

            for i in args1:
                grouped_json[iters[0]][i] += float(iters[i+1])

                grouped_json[iters[0]][-1] += 1
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.submit(second_loop,zip(*[table[u] for u in flattened]),range(len(apply_on_list)))

    '''
    for iters in zip(*[table[u] for u in flattened]):
        if iters[0] not in grouped_json:
            grouped_json[iters[0]] = np.zeros(len(apply_on_list) + 1)

        for i in range(len(apply_on_list)):
            grouped_json[iters[0]][i] += float(iters[i+1])

            grouped_json[iters[0]][-1] += 1
    '''

    if operation == 'mean':
        def third_loop(args,args1):
            for k in args:
                for i in args1:
                    grouped_json[k][i] = grouped_json[k][i] / grouped_json[k][-1]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(third_loop,grouped_json.keys(),range(len(apply_on_list)))
        '''
        for k in grouped_json.keys():
            for i in range(len(apply_on_list)):
                grouped_json[k][i] = grouped_json[k][i] / grouped_json[k][-1]
        '''

    grouped_df = order(grouped_json, how, flattened)
    duration = time.time() - start
    go_plot(grouped_df, group_by_list, apply_on_list, how)
    print(f'Time spent for group by on columns: {duration}')
    return grouped_df


# --------------- GROUPING ROW STORAGE DISK --------------------------

def row_group_by(file_name, group_by_list, apply_on_list, operation='sum', how=None):
    start = time.time()
    flattened = [u for subitem in [group_by_list, apply_on_list] for u in subitem]

    file_path = os.path.join(os.getcwd(), 'disk_storage_row\\ ' + file_name)
    with open(file_path, 'r') as json_file:
        table = json.load(json_file)

    iters = []
    def first_loop(args,args1):
        while len(args) < len(args1):
            ran = random.choice(string.ascii_letters)
            if ran not in iters:
                iters.append(ran)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.submit(first_loop,iters,flattened)
    '''  
    while len(iters) < len(flattened):
        ran = random.choice(string.ascii_letters)
        if ran not in iters:
            iters.append(ran)
    '''

    grouped_json = {}
    def second_loop(args,args1):
        for row in args:
            if row[flattened[0]] not in grouped_json:
                grouped_json[row[flattened[0]]] = np.zeros(len(apply_on_list) + 1)
            for i in args1:
                grouped_json[row[flattened[0]]][i] += float(row[flattened[i+1]])
                grouped_json[row[flattened[0]]][-1] += 1
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.submit(second_loop,table['rows'],range(len(apply_on_list)))
    '''
    for row in table['rows']:
        if row[flattened[0]] not in grouped_json:
            grouped_json[row[flattened[0]]] = np.zeros(len(apply_on_list) + 1)
        for i in range(len(apply_on_list)):
            grouped_json[row[flattened[0]]][i] += float(row[flattened[i+1]])
            grouped_json[row[flattened[0]]][-1] += 1
    '''

    if operation == 'mean':
        def third_loop(args,args1):
            for k in args:
                for i in args1:
                    grouped_json[k][i] = grouped_json[k][i] / grouped_json[k][-1]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(third_loop,grouped_json.keys(),range(len(apply_on_list)))
        '''
        for k in grouped_json.keys():
            for i in range(len(apply_on_list)):
                grouped_json[k][i] = grouped_json[k][i] / grouped_json[k][-1]
        '''

    grouped_df = order(grouped_json, how, flattened)
    duration = time.time() - start
    go_plot(grouped_df, group_by_list, apply_on_list, how)
    print(f'Time spend for group by on row: {duration}')
    return grouped_df


col_group_by('LINEITEM_column.txt', ['suppkey'], ['quantity', 'discount'], operation='mean')
row_group_by('LINEITEM_row.txt', ['suppkey'], ['quantity'], operation='mean', how="DESC")



