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
      
     def show_table(self,max_col=0):
        dict_table = copy.deepcopy(self.__dict__)
        del dict_table['row_version']
        del dict_table['name_table']
        del dict_table['disk']
        prim_key=dict_table['primary_key_name']
        second_key=dict_table['foreign_key_name']
        del dict_table['primary_key_name']
        del dict_table['foreign_key_name']
        r_lab=[]
        cell_txt=[]
        for attribute, val in zip(dict_table.keys(), dict_table.values()):
            r_lab.append(attribute)
            cell_txt.append(val)
        fig, (ax1,ax2) = plt.subplots(2) 
        ax1.set_axis_off() 
        ax2.set_axis_off()
        if max_col!=0:
            for i in range(len(cell_txt)):
                cell_txt[i]=cell_txt[i][:max_col]
        idx=[]
        k=0
        for elem in cell_txt:
            keep=False
            for i in range(len(elem)):
                if elem[i]!="":
                    keep=True
            if keep==False:
                cell_txt.remove(elem)
                idx.append(k)
            k+=1
        r_lab_res=copy.deepcopy(r_lab)
        for elem in idx:
            r_lab_res.remove(r_lab[elem])
        table = ax1.table( 
        cellText = cell_txt,  
        rowLabels = r_lab_res,   
        rowColours =["palegreen"] * len(r_lab),
        rowLoc='center',  
        cellLoc ='center',  
        loc ='upper left')
        table1 = ax2.table( 
        cellText = [prim_key,second_key],  
        rowLabels = ["Primary keys","Foreign keys"],   
        rowColours =["red"] * 2,
        colWidths=[0.75],
        rowLoc='center',  
        cellLoc ='center',  
        loc ='upper right')         

        
        ax1.set_title(self.name_table+"_column", 
            fontweight ="bold")
        ax2.set_title("Primary keys and foreign keys", 
            fontweight ="bold")
        plt.show()

    def projection(self,you,attributes=[]):
        proj=copy.deepcopy(self.__dict__)
        del proj['row_version']
        del proj['name_table']
        del proj['disk']
        del proj['primary_key_name']
        del proj['foreign_key_name']
        keys=list(proj.keys())
        nb_keys=len(keys)
        key_del=[]
        for key in proj.keys():
            if key not in attributes:
                key_del.append(key)
        for key in key_del:
            del proj[key]
        addings=len(list(proj.values())[0])
        adding=[]
        for i in range(addings):
            temp=["" for i in range(nb_keys)]
            for j in range(len(attributes)):
                pos=keys.index(attributes[j])
                temp[pos]=proj[attributes[j]][i]
            adding.append(temp)
        for elem in adding:
            you.add(elem)
        locname=""
        for att in attributes:
            locname+=att+"_"
        you.disk = DISK_STORAGE_COLUMN + self.name_table + '_projection_'+locname+'column.txt'
        you.dump_column()

    def select_table(self,you,conditions=("name",'==', "Algeria")):
        dict_table = copy.deepcopy(self.__dict__)
        del dict_table['row_version']
        del dict_table['name_table']
        del dict_table['disk']
        del dict_table['primary_key_name']
        del dict_table['foreign_key_name']

        attribut=[]
        valeur=[]

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

        conditionned_att=[]
        valeur_t={}
        vall_t=[]
        attribut_t=[]
        id=[]

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
            vall1=[]
            if attribut[i] != conditions[0]:
                id.append(i)
                attribut_t.append(attribut[i])
                vall1=[valeur[i][j] for j in index]
                vall_t.append(vall1)


        att = [x for _, x in sorted(zip(id, attribut_t))]
        val= [x for _, x in sorted(zip(id, vall_t))]

        for i in range(len(att)):
            valeur_t[att[i]] = val[i]
        adding=[]
        vals= list(valeur_t.values())
        for i in range(len(vals[0])):
            temp=[]
            for elem in vals:
                temp.append(elem[i])
            adding.append(temp)
        for elem in adding:
            you.add(elem)
        you.disk= DISK_STORAGE_COLUMN + 'select_tables_class.txt'
        you.dump_column()


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
