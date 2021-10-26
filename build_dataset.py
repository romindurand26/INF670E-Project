
SQL_DATABASE = '/home/romin/Documents/M2 Data Science/Systems for big data analytics/data_sql/s-0.01/supplier.sql'
OUTPUT_FILE = '/home/romin/Documents/M2 Data Science/Systems for big data ' \
              'analytics/INF670E-Project/fill_dataset/fill_supplier.py'
out_string = 'from TPC_H_structure.supplier import Supplier_column, Supplier_row \nc_col = Supplier_column() \nc_raw ' \
             '= Supplier_row() \n'


with open(SQL_DATABASE, 'r') as file:
    lines = file.readlines()

for p, command in enumerate(lines):
    if p % 100 == 0:
        print(p)
    values = []
    data_row = command.split(' VALUES (')[1]
    data_row = data_row[:-3:]
    n = len(data_row)
    i = 0
    while i < n:
        car = data_row[i]
        current_value = ''

        if car == "'":
            i += 1
            car = data_row[i]
            while car != "'":
                current_value += car
                i += 1
                car = data_row[i]
            values.append(current_value)
            i += 2

        else:
            while car != ",":
                current_value += car
                i += 1
                car = data_row[i]
            values.append(current_value)
            i += 1

    out_string += 'c_col.add({}) \n'.format(values)
    out_string += 'c_raw.add({}) \n'.format(values)

out_string += 'c_col.dump_column() \n'
out_string += 'c_raw.dump_row() \n'

with open(OUTPUT_FILE, 'w') as file:
    file.write(out_string)
