import json


# column version


class Table_column:
    def __init__(self, file):
        self.primary_key_name = None
        self.__load(file)

    def add(self, values):
        for attribute, val in zip(self.__dict__.keys(), [None] + values):
            if attribute == 'primary_key_name':
                continue
            getattr(self, attribute).append(val)

    def dump_column(self, file):
        dict = self.__dict__

        with open(file, 'w') as json_file:
            json.dump(dict, json_file)

    def __load(self, file):
        with open(file, 'r') as json_file:
            data = json.load(json_file)

        for attribute in data.keys():
            setattr(self, attribute, data[attribute])


class Customers_column(Table_column):
    def __init__(self, file):
        super().__init__(file)


file1 = "/home/romin/Documents/M2 Data Science/Systems for big data analytics/INF670E-Project/json_column.txt"
customers = Customers_column(file1)
customers.add(["111", "Hubert", "55"])
customers.add(["222", "Jean", "1"])

customers.dump_column(file1)
