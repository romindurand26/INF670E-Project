import json
import os
import pandas as pd

root_path = os.getcwd()

customer_col = os.path.join(root_path, 'disk_storage_column\\ CUSTOMER_column.txt')
order_col = os.path.join(root_path, 'disk_storage_column\\ ORDERS_column.txt')
lineItem_col = os.path.join(root_path, 'disk_storage_column\\ LINEITEM_column.txt')

with open(customer_col, 'r') as json_file:
    cust_col = json.load(json_file)

with open(order_col, 'r') as json_file:
    order_col = json.load(json_file)

with open(lineItem_col, 'r') as json_file:
    lineItem_col = json.load(json_file)

'''table = order_col
group_by = 'custkey'
apply_on = 'totalprice'

grouped_json = {}
for key, val in zip(table[group_by], table[apply_on]):
    if key not in grouped_json:
        grouped_json[key] = 0
    grouped_json[key] += float(val)

grouped_df = pd.DataFrame(
    list(zip(list(grouped_json.keys()), list(grouped_json.values()))),
    columns=[group_by, apply_on])
print(grouped_df)'''

table = lineItem_col
group_by = ['suppkey']
apply_on = ['quantity', 'discount']

zip_l = [item for subitem in [group_by, apply_on] for item in subitem]
#print((table[k] for k in zip_l))
grouped_json = {}

for key, val1, val2 in zip(table[k] for k in zip_l):
    if key not in grouped_json:
        grouped_json[key] = [0, 0]
    for i in range(len(apply_on)):
        grouped_json[key][i] += float(val1)
        grouped_json[key][i] += float(val2)

print(grouped_json)


'''grouped_df = pd.DataFrame(
    list(zip(list(grouped_json.keys()), list(grouped_json.values()))),
    columns=[group_by, apply_on])
print(grouped_df)'''


