from TPC_H_structure.customer import Customer_column, Customer_row

SQL_DATABASE = '/home/romin/Documents/M2 Data Science/Systems for big data analytics/data_sql/s-0.01/customer.sql'
c_col = Customer_column()
c_raw = Customer_row()

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

    c_col.add(values)
    c_raw.add(values)

c_col.dump_column()
c_raw.dump_row()
