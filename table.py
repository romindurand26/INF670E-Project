import json
import copy
import os

DISK_STORAGE_COLUMN = '/home/romin/Documents/M2 Data Science/Systems for big data ' \
                      'analytics/INF670E-Project/disk_storage_column/ '
DISK_STORAGE_ROW = '/home/romin/Documents/M2 Data Science/Systems for big data ' \
                   'analytics/INF670E-Project/disk_storage_row/ '


# column version


class Table_column:
    def __init__(self, row_version, name_table='universal_table'):
        self.row_version = row_version
        self.name_table = name_table
        self.disk = DISK_STORAGE_COLUMN + self.name_table + '_column.txt'

        self.__check_disk()
        self.__load()

    def __check_disk(self):
        if not os.path.isfile(self.disk):
            self.dump_column()

    def __load(self):
        with open(self.disk, 'r') as json_file:
            data = json.load(json_file)

        for attribute in data.keys():
            setattr(self, attribute, data[attribute])

    def add(self, values):
        dict_table = copy.deepcopy(self.__dict__)
        del dict_table['row_version']
        del dict_table['name_table']
        del dict_table['disk']

        del dict_table['primary_key_name']
        del dict_table['foreign_key_name']

        for attribute, val in zip(dict_table.keys(), values):
            getattr(self, attribute).append(val)

    def dump_column(self):
        dict_dump = copy.deepcopy(self.__dict__)
        del dict_dump['row_version']
        del dict_dump['name_table']
        del dict_dump['disk']

        with open(self.disk, 'w') as json_file:
            json.dump(dict_dump, json_file)

    def to_row(self):
        t_row = self.row_version()
        dict_table = copy.deepcopy(self.__dict__)
        del dict_table['row_version']
        del dict_table['name_table']
        del dict_table['disk']

        del dict_table['primary_key_name']
        del dict_table['foreign_key_name']

        for attribute in dict_table.keys():
            n = len(getattr(self, attribute))

        for i in range(n):
            values = []
            for attribute in dict_table.keys():
                values.append(getattr(self, attribute)[i])
            t_row.add(values)

        return t_row


# row version


class Row:
    def __init__(self, values):
        for attribute, val in zip(self.__dict__.keys(), values):
            setattr(self, attribute, val)


class Table_row:
    def __init__(self, column_version, class_row, name_table='universal_table'):
        self.column_version = column_version
        self.class_row = class_row
        self.name_table = name_table
        self.disk = DISK_STORAGE_ROW + self.name_table + '_row.txt'
        self.rows = []

        self.__check_disk()
        self.__load()

    def __check_disk(self):
        if not os.path.isfile(self.disk):
            self.dump_row()

    def __load(self):
        with open(self.disk, 'r') as json_file:
            data = json.load(json_file)
        setattr(self, "primary_key_name", data["primary_key_name"])
        setattr(self, "foreign_key_name", data["foreign_key_name"])

        rows = data["rows"]
        for data_row in rows:
            values = []
            for attribute in data_row.keys():
                values.append(data_row[attribute])
            row = self.class_row(values)
            self.rows.append(row)

    def add(self, values):
        self.rows.append(self.class_row(values))

    def dump_row(self):
        list_row = []
        for r in self.rows:
            dict_r = r.__dict__
            list_row.append(dict_r)

        dict_dump = copy.deepcopy(self.__dict__)
        del dict_dump['column_version']
        del dict_dump['class_row']
        del dict_dump['name_table']
        del dict_dump['disk']
        dict_dump['rows'] = list_row

        with open(self.disk, 'w') as json_file:
            json.dump(dict_dump, json_file)

    def to_column(self):
        t_column = self.column_version()
        dict_table = copy.deepcopy(self.__dict__)
        del dict_table['column_version']
        del dict_table['class_row']
        del dict_table['name_table']
        del dict_table['disk']

        list_row = dict_table['rows']

        for r in list_row:
            dict_r = r.__dict__
            values = []
            for attribute in dict_r.keys():
                values.append(getattr(r, attribute))
            t_column.add(values)

        return t_column
