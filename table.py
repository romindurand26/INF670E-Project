import json
import copy
import os

DISK_STORAGE_COLUMN = '/home/romin/Documents/M2 Data Science/Systems for big data ' \
                      'analytics/INF670E-Project/disk_storage_column/ '
DISK_STORAGE_ROW = '/home/romin/Documents/M2 Data Science/Systems for big data ' \
                   'analytics/INF670E-Project/disk_storage_row/ '


# column version


class Table_column:
    """
        Class generalizing the notion of a table stored in a column way.

        ...

        Attributes
        ----------
        row_version : function class Table_row()
            The row version of the table.
        name_table : string
            The name of the table.
        disk : string
            The path to the disk for the storage.

        Methods
        -------
        __check_disk()
            Check that the file of the path to the disk exist, and initialize it if not.
        __load()
            Load the data in the disk.
        add(values)
            Add one row to the table.
        dump_column()
            Dump the data to the disk in a column way.
        to_row()
            Convert the Table_column to the Table_row

        """
    def __init__(self, row_version, name_table='universal_table'):
        self.row_version = row_version
        self.name_table = name_table
        self.disk = DISK_STORAGE_COLUMN + self.name_table + '_column.txt'

        self.__check_disk()
        self.__load()

    def __check_disk(self):
        """
        __check_disk()
            Check that the file of the path to the disk exist, and initialize it if not.
        :return: nothing
        """
        if not os.path.isfile(self.disk):
            self.dump_column()

    def __load(self):
        """
         __load()
            Load the data in the disk.
        :return: nothing
        """
        with open(self.disk, 'r') as json_file:
            data = json.load(json_file)

        for attribute in data.keys():
            setattr(self, attribute, data[attribute])

    def add(self, values):
        """
        add(values)
            Add one row to the table.
        :param values: list of values in the new row
        :return: nothing
        """
        dict_table = copy.deepcopy(self.__dict__)
        del dict_table['row_version']
        del dict_table['name_table']
        del dict_table['disk']

        del dict_table['primary_key_name']
        del dict_table['foreign_key_name']

        for attribute, val in zip(dict_table.keys(), values):
            getattr(self, attribute).append(val)

    def dump_column(self):
        """
        dump_column()
            Dump the data to the disk in a column way.
        :return: nothing
        """
        dict_dump = copy.deepcopy(self.__dict__)
        del dict_dump['row_version']
        del dict_dump['name_table']
        del dict_dump['disk']

        with open(self.disk, 'w') as json_file:
            json.dump(dict_dump, json_file)

    def to_row(self):
        """
        to_row()
            Convert the Table_column to the Table_row
        :return: t_row the table in row version
        """
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
    """
        Class generalizing the notion of a table stored in a row way.

        ...

        Attributes
        ----------
        column_version : function class Table_column()
            The column version of the table.
        class_row : class row
            The row class fo this table
        name_table : string
            The name of the table.
        disk : string
            The path to the disk for the storage.
        rows : list of class Row
            List of the rows of the table

        Methods
        -------
        __check_disk()
            Check that the file of the path to the disk exist, and initialize it if not.
        __load()
            Load the data in the disk.
        add(values)
            Add one row to the table.
        dump_row()
            Dump the data to the disk in a row way.
        to_column()
            Convert the Table_row to the Table_column

    """
    def __init__(self, column_version, class_row, name_table='universal_table'):
        self.column_version = column_version
        self.class_row = class_row
        self.name_table = name_table
        self.disk = DISK_STORAGE_ROW + self.name_table + '_row.txt'
        self.rows = []

        self.__check_disk()
        self.__load()

    def __check_disk(self):
        """
        __check_disk()
            Check that the file of the path to the disk exist, and initialize it if not.
        :return: nothing
        """
        if not os.path.isfile(self.disk):
            self.dump_row()

    def __load(self):
        """
        __load()
            Load the data in the disk.
        :return: nothing
        """
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
        """
        add(values)
            Add one row to the table.
        :param values: list of values in the new row
        :return: nothing
        """
        self.rows.append(self.class_row(values))

    def dump_row(self):
        """
        dump_row()
            Dump the data to the disk in a row way.
        :return: nothing
        """
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
        """
        to_column()
            Convert the Table_row to the Table_column
        :return: t_column the table in column version
        """
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
