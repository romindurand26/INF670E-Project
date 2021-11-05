import json
import copy
import os
import operator
import matplotlib.pyplot as plt

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

    def show_table(self, max_col=0):
        dict_table = copy.deepcopy(self.__dict__)
        del dict_table['row_version']
        del dict_table['name_table']
        del dict_table['disk']
        prim_key = dict_table['primary_key_name']
        second_key = dict_table['foreign_key_name']
        del dict_table['primary_key_name']
        del dict_table['foreign_key_name']
        r_lab = []
        cell_txt = []

        for attribute, val in zip(dict_table.keys(), dict_table.values()):
            r_lab.append(attribute)
            cell_txt.append(val)

        fig, (ax1, ax2) = plt.subplots(2)
        ax1.set_axis_off()
        ax2.set_axis_off()
        if max_col != 0:
            for i in range(len(cell_txt)):
                cell_txt[i] = cell_txt[i][:max_col]

        idx = []
        k = 0
        for elem in cell_txt:
            keep = False
            for i in range(len(elem)):
                if elem[i] != "":
                    keep = True
            if keep == False:
                cell_txt.remove(elem)
                idx.append(k)
            k += 1

        r_lab_res = copy.deepcopy(r_lab)
        for elem in idx:
            r_lab_res.remove(r_lab[elem])

        table = ax1.table(
            cellText=cell_txt,
            rowLabels=r_lab_res,
            rowColours=["palegreen"] * len(r_lab),
            rowLoc='center',
            cellLoc='center',
            loc='upper left')
        table1 = ax2.table(
            cellText=[prim_key, second_key],
            rowLabels=["Primary keys", "Foreign keys"],
            rowColours=["red"] * 2,
            colWidths=[0.75],
            rowLoc='center',
            cellLoc='center',
            loc='upper right')

        ax1.set_title(self.name_table + "_column",
                      fontweight="bold")
        ax2.set_title("Primary keys and foreign keys",
                      fontweight="bold")
        plt.show()

    def projection(self, you, attributes=[]):
        proj = copy.deepcopy(self.__dict__)
        del proj['row_version']
        del proj['name_table']
        del proj['disk']
        del proj['primary_key_name']
        del proj['foreign_key_name']
        keys = list(proj.keys())
        nb_keys = len(keys)
        key_del = []
        for key in proj.keys():
            if key not in attributes:
                key_del.append(key)
        for key in key_del:
            del proj[key]
        addings = len(list(proj.values())[0])
        adding = []
        for i in range(addings):
            temp = ["" for i in range(nb_keys)]
            for j in range(len(attributes)):
                pos = keys.index(attributes[j])
                temp[pos] = proj[attributes[j]][i]
            if temp not in adding:
                adding.append(temp)
        for elem in adding:
            you.add(elem)
        locname = ""
        for att in attributes:
            locname += att + "_"
        you.disk = DISK_STORAGE_COLUMN + self.name_table + '_projection_' + locname + 'column.txt'
        you.dump_column()

    def select_table(self, you, conditions=("name", '==', "Algeria")):
        dict_table = copy.deepcopy(self.__dict__)
        del dict_table['row_version']
        del dict_table['name_table']
        del dict_table['disk']
        del dict_table['primary_key_name']
        del dict_table['foreign_key_name']

        attribut = []
        valeur = []

        ops = {
            '==': operator.eq,
            '!=': operator.ne,
            '<': operator.lt,
            '>': operator.le,  # use operator.div for Python 2
            '>=': operator.gt,
            '<=': operator.ge,
        }

        for attribute, val in zip(dict_table.keys(), dict_table.values()):
            attribut.append(attribute)
            valeur.append(val)

        conditionned_att = []
        valeur_t = {}
        vall_t = []
        attribut_t = []
        id = []

        for i in range(len(attribut)):
            if attribut[i] == conditions[0]:
                id.append(i)
                attribut_t.append(attribut[i])
                vall = []
                index = []
                for j in range(len(valeur[i])):
                    if ops[conditions[1]](valeur[i][j], conditions[2]):
                        vall.append(valeur[i][j])
                        index.append(j)
                vall_t.append(vall)

        for i in range(len(attribut)):
            vall1 = []
            if attribut[i] != conditions[0]:
                id.append(i)
                attribut_t.append(attribut[i])
                vall1 = [valeur[i][j] for j in index]
                vall_t.append(vall1)

        att = [x for _, x in sorted(zip(id, attribut_t))]
        val = [x for _, x in sorted(zip(id, vall_t))]

        for i in range(len(att)):
            valeur_t[att[i]] = val[i]
        adding = []
        vals = list(valeur_t.values())
        for i in range(len(vals[0])):
            temp = []
            for elem in vals:
                temp.append(elem[i])
            adding.append(temp)
        for elem in adding:
            you.add(elem)
        you.disk = DISK_STORAGE_COLUMN + 'select_tables_class.txt'
        you.dump_column()

    def join(self, you, jointure, join_type, theta=[]):
        dict_table = copy.deepcopy(self.__dict__)
        dict_table1 = copy.deepcopy(you.__dict__)
        del dict_table['row_version']
        del dict_table['name_table']
        del dict_table['disk']
        del dict_table['primary_key_name']
        del dict_table['foreign_key_name']
        del dict_table1['row_version']
        del dict_table1['name_table']
        del dict_table1['disk']
        del dict_table1['primary_key_name']
        del dict_table1['foreign_key_name']
        attribut = []
        valeur = []
        attribut1 = []
        valeur1 = []
        for attribute, val in zip(dict_table.keys(), dict_table.values()):
            attribut.append(attribute)
            valeur.append(val)
        for attribute, val in zip(dict_table1.keys(), dict_table1.values()):
            attribut1.append(attribute)
            valeur1.append(val)
        ## verification if join is possible for natural join
        if join_type == "natural":
            common_att = []
            for i in range(len(attribut)):
                for j in range(len(attribut1)):
                    if attribut[i] == attribut1[j]:
                        common_att.append((attribut[i], i, j))
            for elem in common_att:
                if elem[0] not in theta:
                    idx = attribut.index(elem[0])
                    idx1 = attribut1.index(elem[0])
                    attribut[idx] += self.name_table
                    attribut1[idx1] += you.name_table
            possible = True
            for elem in theta:
                idx1 = attribut.index(elem)
                idx2 = attribut1.index(elem)
                val = valeur[idx1]
                val1 = valeur[idx2]
                if len(val) != len(val1):
                    possible = False
                for v in val:
                    if v not in val1:
                        possible = False
            if (possible == False):
                print("natural join is not possible in this case")
            else:
                attribut2 = copy.deepcopy(attribut1)
                valeur2 = copy.deepcopy(valeur1)
                for elem in theta:
                    vals = valeur[attribut.index(elem)]
                    for v in vals:
                        target_idx = vals.index(v)
                        current_idx = valeur2[attribut2.index(elem)].index(v)
                        for element in valeur2:
                            element[current_idx], element[target_idx] = element[target_idx], element[current_idx]
                for elem in theta:
                    idx = attribut2.index(elem)
                    del attribut2[idx]
                    del valeur2[idx]
                attribut += attribut2
                valeur += valeur2
                for key in attribut:
                    jointure.add_key(key)
                for val in valeur:
                    jointure.add_values(val)
                jointure.dump()
        else:
            # verification
            common_att = []
            for i in range(len(attribut)):
                for j in range(len(attribut1)):
                    if attribut[i] == attribut1[j]:
                        common_att.append((attribut[i], i, j))
            for elem in common_att:
                idx = attribut.index(elem[0])
                idx1 = attribut1.index(elem[0])
                attribut[idx] += self.name_table
                attribut1[idx1] += you.name_table
            theta0 = theta
            if theta0[0] in common_att:
                theta0[0] += self.name_table
            if theta0[1] in common_att:
                theta0[1] += you.name_table
            possible = True
            if len(valeur[0]) >= len(valeur1[0]):
                vals = valeur[attribut.index(theta0[0])]
                vals1 = valeur1[attribut1.index(theta0[1])]
                for v in vals1:
                    if v not in vals:
                        possible = False
            else:
                vals = valeur[attribut.index(theta0[0])]
                vals1 = valeur1[attribut1.index(theta0[1])]
                for v in vals:
                    if v not in vals1:
                        possible = False
            if possible == False:
                print("theta join is not possible in this case")
            else:
                if len(valeur[0]) >= len(valeur1[0]):
                    new_vals = [[] for i in range(len(attribut1))]
                    target_vals = valeur[attribut.index(theta0[0])]
                    for v in target_vals:
                        for j in range(len(valeur1[attribut1.index(theta0[1])])):
                            if valeur1[attribut1.index(theta0[1])][j] == v:
                                for i in range(len(new_vals)):
                                    new_vals[i].append(valeur1[i][j])
                    attribut += attribut1
                    valeur += new_vals
                    for key in attribut:
                        jointure.add_key(key)
                    for val in valeur:
                        jointure.add_values(val)
                    jointure.dump()
                else:
                    new_vals = [[] for i in range(len(attribut))]
                    target_vals = valeur1[attribut1.index(theta0[1])]
                    for v in target_vals:
                        for j in range(len(valeur[attribut.index(theta0[0])])):
                            if valeur[attribut.index(theta0[0])][j] == v:
                                for i in range(len(new_vals)):
                                    new_vals[i].append(valeur[i][j])
                    attribut1 += attribut
                    valeur1 += new_vals
                    for key in attribut1:
                        jointure.add_key(key)
                    for val in valeur1:
                        jointure.add_values(val)
                    jointure.dump()


class jointure_table():
    def __init__(self, keys=[], val=[]):
        self.keys = keys
        self.values = val
        self.disk = DISK_STORAGE_COLUMN + 'joint_tables.txt'

    def add_key(self, key):
        self.keys.append(key)

    def add_values(self, val):
        self.values.append(val)

    def dump(self):
        keys_list = self.keys
        values_list = self.values
        zip_iterator = zip(keys_list, values_list)
        a_dictionary = dict(zip_iterator)
        with open(self.disk, 'w') as json_file:
            json.dump(a_dictionary, json_file)

    def show_joint(self):
        dict_j = {}
        for i in range(len(self.keys)):
            dict_j[self.keys[i]] = self.values[i]
        print(dict_j)


# row version
class jointure_table_row():
    def __init__(self):
        self.disk = DISK_STORAGE_ROW + 'joint_tables.txt'
        self.rows = []

    def add_rows(self, row):
        self.rows.append(row)

    def dump_row(self):
        list_row = []
        for r in self.rows:
            print(r)
            dict_r = r
            list_row.append(dict_r)
        data = {}
        data['rows'] = list_row
        print(data)
        with open(self.disk, 'w') as json_file:
            json.dump(data, json_file)


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

    def show_table(self, max_row=0):
        cell_txt = []
        cols = list(self.rows[0].__dict__.keys())
        for r in self.rows:
            dict_r = r.__dict__
            cell_txt.append(list(dict_r.values()))
        prim_key = self.__dict__["primary_key_name"]
        second_key = self.__dict__["foreign_key_name"]
        fig, (ax1, ax2) = plt.subplots(2)
        ax1.set_axis_off()
        ax2.set_axis_off()
        if max_row != 0:
            cell_txt = cell_txt[:max_row]
        cols_res = copy.deepcopy(cols)
        cell_txt_res = copy.deepcopy(cell_txt)
        for i in range(len(cols)):
            keep = False
            for elem in cell_txt:
                if elem[i] != "":
                    keep = True
            if keep == False:
                cols_res.remove(cols[i])
                for elem in cell_txt_res:
                    elem.remove("")

        table = ax1.table(
            cellText=cell_txt_res,
            colLabels=cols_res,
            colColours=["palegreen"] * len(cols),
            colLoc='center',
            cellLoc='center',
            loc='upper left')
        table1 = ax2.table(
            cellText=[prim_key, second_key],
            rowLabels=["Primary keys", "Foreign keys"],
            rowColours=["red"] * 2,
            colWidths=[0.75],
            rowLoc='center',
            cellLoc='center',
            loc='upper right')

        ax1.set_title(self.name_table + "_row",
                      fontweight="bold")
        ax2.set_title("Primary keys and foreign keys",
                      fontweight="bold")
        plt.show()

    def projection(self, you, attributes=[]):
        values = []
        cols = list(self.rows[0].__dict__.keys())
        for r in self.rows:
            dict_r = r.__dict__
            values.append(list(dict_r.values()))
        adding = []
        edx = []
        for att in attributes:
            edx.append(cols.index(att))
        adding = []
        for i in range(len(values)):
            temp = ["" for j in range(len(cols))]
            for elem in edx:
                temp[elem] = values[i][elem]
            if temp not in adding:
                adding.append(temp)
        for elem in adding:
            you.add(elem)
        locname = ""
        for att in attributes:
            locname += att + "_"
        you.disk = DISK_STORAGE_ROW + self.name_table + '_projection_' + locname + 'row.txt'
        you.dump_row()

    def select_table_row(self, conditions=("name", '==', "Algeria")):
        list_row = []
        for r in self.rows:
            dict_r = r.__dict__
            list_row.append(dict_r)

        data = copy.deepcopy(self.__dict__)
        del data['column_version']
        del data['class_row']
        del data['name_table']
        del data['disk']
        data['rows'] = list_row
        rows = data['rows']

        ops = {
            '==': operator.eq,
            '!=': operator.ne,
            '<': operator.lt,
            '>': operator.le,  # use operator.div for Python 2
            '>=': operator.gt,
            '<=': operator.ge,
        }
        list_row_t = []
        values = []
        attribut = []
        for data_row in rows:
            for attribute in data_row.keys():
                if (attribute == conditions[0] and ops[conditions[1]](data_row[attribute], conditions[2])):
                    list_row_t.append(data_row)
                    values.append(data_row[attribute])

        disk = DISK_STORAGE_ROW + 'select_tables.txt'
        with open(disk, 'w') as json_file:
            json.dump(list_row_t, json_file)

    def join_row(self, you, join_type, theta=[]):

        list_row = []
        for r in self.rows:
            dict_r = r._dict_
            list_row.append(dict_r)

        dict_dump = copy.deepcopy(self._dict_)
        del dict_dump['column_version']
        del dict_dump['class_row']
        del dict_dump['name_table']
        del dict_dump['disk']
        dict_dump['rows'] = list_row
        rows = dict_dump['rows']

        list_row1 = []
        for r in you.rows:
            dict_r = r._dict_
            list_row1.append(dict_r)

        dict_dump1 = copy.deepcopy(you._dict_)
        del dict_dump1['column_version']
        del dict_dump1['class_row']
        del dict_dump1['name_table']
        del dict_dump1['disk']
        dict_dump1['rows'] = list_row1
        rows1 = dict_dump1['rows']
        print(len(rows))
        print(len(rows1))

        if join_type == "theta":
            attribut = []
            attribut1 = []
            for data_row in rows[0]:
                attribut.append(data_row)
            for data_row1 in rows1[0]:
                attribut1.append(data_row1)
            List_rows = []
            print(len(rows))
            print(len(rows1))
            if len(rows) >= len(rows1):
                print(List_rows)
                for i in range(len(rows)):
                    for k in range(len(rows1)):
                        if rows[i][theta[0]] == rows1[k][theta[1]]:
                            for n in range(len(list(rows1[k].keys()))):
                                if list(rows1[k].keys())[n] in attribut:
                                    rows[i][list(rows1[k].keys())[n] + '1'] = rows1[k][list(rows1[k].keys())[n]]
                                else:
                                    rows[i][list(rows1[k].keys())[n]] = rows1[k][list(rows1[k].keys())[n]]

                List_rows.append(rows)

            else:
                List_rows.append(rows1)
                for i in range(len(rows1)):
                    for k in range(len(rows)):
                        if rows1[i][theta[1]] == rows[k][theta[0]]:
                            for n in range(len(list(rows[k].keys()))):
                                if list(rows[k].keys())[n] in attribut1:
                                    rows1[i][list(rows[k].keys())[n] + '1'] = rows[k][list(rows[k].keys())[n]]
                                else:
                                    rows1[i][list(rows[k].keys())[n]] = rows[k][list(rows[k].keys())[n]]
                List_rows.append(rows1)
            print(List_rows)
            disk = DISK_STORAGE_ROW + 'join_tables_theta.txt'
            with open(disk, 'w') as json_file:
                json.dump(List_rows, json_file)

        if join_type == "natural":
            attribut = []
            attribut1 = []
            for data_row in rows[0]:
                attribut.append(data_row)
            for data_row1 in rows1[0]:
                attribut1.append(data_row1)

            print(attribut)
            print(attribut1)

            possible = True
            for elem in theta:
                if (elem not in attribut) or (elem not in attribut1):
                    print('The databases does not have a column in commun')
                    possible = False
                else:
                    possible = True

            if possible == True:
                index = []
                index1 = []
                for i in range(len(rows)):
                    for j in range(len(rows[i].keys())):
                        Keys = list(rows[i].keys())
                        for k in range(len(rows1)):
                            for l in range(len(rows1[k].keys())):
                                Keys1 = list(rows1[k].keys())
                                if Keys[j] in theta and Keys1[l] in theta:
                                    if rows[i][list(rows[i].keys())[j]] == rows1[k][list(rows1[k].keys())[l]]:
                                        index.append([i, j])
                                        index1.append([k, l])
                List_rows = []
                for i, j in index:
                    valeur_t = {}
                    valeur_t[list(rows[i].keys())[j]] = rows[i][list(rows[i].keys())[j]]
                    for m in range(len(rows[i].keys())):
                        if m != j:
                            valeur_t[list(rows[i].keys())[m]] = rows[i][list(rows[i].keys())[m]]
                    List_rows.append(valeur_t)

                s = 0
                for k, l in index1:
                    print(List_rows[s])
                    for n in range(len(rows1[k].keys())):
                        if n != l:
                            if list(rows1[k].keys())[n] in attribut:
                                List_rows[s][list(rows1[k].keys())[n] + '1'] = rows1[k][list(rows1[k].keys())[n]]
                            else:
                                List_rows[s][list(rows1[k].keys())[n]] = rows1[k][list(rows1[k].keys())[n]]

                    print(List_rows[s])
                    s += 1

                print(List_rows)
                disk = DISK_STORAGE_ROW + 'join_tables.txt'
                with open(disk, 'w') as json_file:
                    json.dump(List_rows, json_file)

