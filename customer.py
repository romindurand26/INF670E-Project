import json

class Customer:
    def __init__(self, id, name, age):
        self.id = id
        self.name = name
        self.age = age

class Customers:
    def __init__(self):
        self.customers = []

    def add(self, id, name, age):
        self.customers.append(Customer(id, name, age))

    def dump_row(self, file):
        list_row = []
        for c in self.customers:
            dict = {"id": c.id, "name": c.name, "age": c.age}
            list_row.append(dict)

        with open(file, 'w') as json_file:
            json.dump(list_row, json_file)

    def dump_column(self, file):
        dict_column = {"ids": [], "names": [], "ages": []}
        for c in self.customers:
            dict_column["ids"].append(c.id)
            dict_column["names"].append(c.name)
            dict_column["ages"].append(c.age)

        with open(file, 'w') as json_file:
            json.dump(dict_column, json_file)


customers = Customers()
customers.add("002", "Yasmine", "24")
customers.add("007", "Romin", "77")
customers.add("000", "Oumaima", "5")
customers.add("001", "Louay", "18")

file1 = "/home/romin/Documents/M2 Data Science/Systems for big data analytics/json_row.txt"
customers.dump_row(file1)

file2 = "/home/romin/Documents/M2 Data Science/Systems for big data analytics/json_column.txt"
customers.dump_column(file2)

class Row:
    def __init__(self, primary_key):
        self.primary_key = primary_key

class Table:
    def __init__(self, primary_key):
        self.primary_key = primary_key
        self.rows

    def dump_row(self, file):
        list_row = []
        for c in self.customers:
            dict = {"id": c.id, "name": c.name, "age": c.age}
            list_row.append(dict)

        with open(file, 'w') as json_file:
            json.dump(list_row, json_file)