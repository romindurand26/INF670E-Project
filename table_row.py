import json


# row version

class Row:
    def __init__(self, values):
        for attribute, val in zip(self.__dict__.keys(), values):
            setattr(self, attribute, val)


class Table_row:
    def __init__(self, file, class_row):
        self.primary_key_name = None
        self.class_row = class_row
        self.rows = []
        self.__load(file)

    def add(self, values):
        self.rows.append(self.class_row(values))

    def dump_row(self, file):
        list_row = []
        for r in self.rows:
            dict_r = r.__dict__
            list_row.append(dict_r)

        dict = {"primary_key_name": self.primary_key_name, "rows": list_row}

        with open(file, 'w') as json_file:
            json.dump(dict, json_file)

    def __load(self, file):
        with open(file, 'r') as json_file:
            data = json.load(json_file)
        setattr(self, "primary_key_name", data["primary_key_name"])

        rows = data["rows"]
        for data_row in rows:
            values = []
            for attribute in data_row.keys():
                values.append(data_row[attribute])
            row = self.class_row(values)
            self.rows.append(row)


class Customer(Row):
    def __init__(self, values):
        self.id = None
        self.name = None
        self.age = None
        super().__init__(values)


class Customers_row(Table_row):
    def __init__(self, file):
        super().__init__(file, Customer)


file1 = "/home/romin/Documents/M2 Data Science/Systems for big data analytics/INF670E-Project/json_row.txt"
customers = Customers_row(file1)
customers.add(["111", "Hubert", "55"])
customers.add(["222", "Jean", "1"])

file2 = "/home/romin/Documents/M2 Data Science/Systems for big data analytics/INF670E-Project/json_row2.txt"
customers.dump_row(file2)
