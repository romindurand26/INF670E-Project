import json
import copy
import os
from table_row import Table_row, Row

DISK_STORAGE_COLUMN = '/home/romin/Documents/M2 Data Science/Systems for big data analytics/INF670E-Project/disk_storage_column/'


# column version


class Table_column:
    def __init__(self, name_table='universal_table'):
        self.name_table = name_table
        self.disk = DISK_STORAGE_COLUMN + self.name_table + '_column.txt'
        self.check_disk()
        self.__load()

    def check_disk(self):
        if not os.path.isfile(self.disk):
            self.dump_column()

    def __load(self):
        with open(self.disk, 'r') as json_file:
            data = json.load(json_file)

        for attribute in data.keys():
            setattr(self, attribute, data[attribute])

    def add(self, values):
        dict_table = copy.deepcopy(self.__dict__)
        del dict_table['disk']
        del dict_table['name_table']
        del dict_table['primary_key_name']
        for attribute, val in zip(dict_table.keys(), values):
            getattr(self, attribute).append(val)

    def dump_column(self):
        dict_dump = copy.deepcopy(self.__dict__)
        del dict_dump['disk']
        del dict_dump['name_table']

        with open(self.disk, 'w') as json_file:
            json.dump(dict_dump, json_file)

    def to_row(self):
        new_table = Table_row()


class Customers_column(Table_column):
    def __init__(self):
        self.primary_key_name = 'id'
        self.id = []
        self.name = []
        self.age = []
        super().__init__('customer')

"""
customers = Customers_column()
customers.add(["002", "Yasmine", "24"])
customers.add(["007", "Romin", "77"])
customers.add(["000", "Oumaima", "5"])
customers.add(["001", "Louay", "18"])
customers.dump_column()
"""

customers = Customers_column()
customers.add(["111", "Hubert", "55"])
customers.add(["222", "Jean", "1"])
customers.dump_column()

