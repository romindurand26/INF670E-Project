import json
import copy
import os
import operator
import concurrent.futures
import time
import matplotlib.pyplot as plt

DISK_STORAGE_COLUMN = 'C:/Users/louay/Desktop/3ATA/projet systems for big data analytics/disk_storage_column/ '
DISK_STORAGE_ROW = 'C:/Users/louay/Desktop/3ATA/projet systems for big data analytics/disk_storage_row/ '


# column version


class Table_column:
    def __init__(self, row_version, name_table='universal_table'):
       # threading.Thread.__init__(self)
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
        
        def first_loop(args):
            for attribute, val in args:
                r_lab.append(attribute)
                cell_txt.append(val)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(first_loop, zip(dict_table.keys(), dict_table.values()))
        '''
        for attribute, val in zip(dict_table.keys(), dict_table.values()):
            r_lab.append(attribute)
            cell_txt.append(val)
            '''
        
        fig, (ax1,ax2) = plt.subplots(2) 
        ax1.set_axis_off() 
        ax2.set_axis_off()
        if max_col!=0:
            def second_loop(args):
                for i in args:
                    cell_txt[i]=cell_txt[i][:max_col] 
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.submit(second_loop, range(len(cell_txt)))
            '''
            for i in range(len(cell_txt)):
                cell_txt[i]=cell_txt[i][:max_col]
            '''
        idx=[]
        k=0
        def third_loop(cell_txt,k):
            for elem in cell_txt:
                keep=False
                for i in range(len(elem)):
                    if elem[i]!="":
                        keep=True
                if keep==False:
                    cell_txt.remove(elem)
                    idx.append(k)
                k+=1
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(third_loop, cell_txt,k)
        '''    
        for elem in cell_txt:
            keep=False
            for i in range(len(elem)):
                if elem[i]!="":
                    keep=True
            if keep==False:
                cell_txt.remove(elem)
                idx.append(k)
            k+=1
        '''
        r_lab_res=copy.deepcopy(r_lab)
        def fourth_loop(args):
            for elem in args:
                r_lab_res.remove(r_lab[elem])
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(fourth_loop, idx)


        '''
        for elem in idx:
            r_lab_res.remove(r_lab[elem])
        '''
        

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
        def first_loop(args):
            for key in args:
                if key not in attributes:
                    key_del.append(key)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(first_loop, proj.keys())
        '''
        for key in proj.keys():
            if key not in attributes:
                key_del.append(key)
        '''
        def second_loop(args):
            for key in args:
                del proj[key]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(second_loop, key_del)
        '''
        for key in key_del:
            del proj[key]
        '''
        addings=len(list(proj.values())[0])
        adding=[]
        def third_loop(args):
            for i in args:
                temp=["" for i in range(nb_keys)]
                for j in range(len(attributes)):
                    pos=keys.index(attributes[j])
                    temp[pos]=proj[attributes[j]][i]

                if temp not in adding:
                    adding.append(temp)
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(third_loop,range(addings))
        '''
        for i in range(addings):
            temp=["" for i in range(nb_keys)]
            for j in range(len(attributes)):
                pos=keys.index(attributes[j])
                temp[pos]=proj[attributes[j]][i]

            if temp not in adding:
                adding.append(temp)
        '''
        def fourth_loop(args):
            for elem in args:
                you.add(elem)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(fourth_loop,adding)
        '''
        for elem in adding:
            you.add(elem)
        '''
        locname=""
        for att in attributes:
            locname+=att+"_"
        you.disk = DISK_STORAGE_COLUMN + self.name_table + '_projection_'+locname+'column.txt'
        you.dump_column()

    def select_table(self,conditions=("name",'==', "Algeria")):
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


        def first_loop(args):
            for attribute, val in args:
                attribut.append(attribute)
                valeur.append(val)
            return attribut, valeur

        start = time.time()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(first_loop, zip(dict_table.keys(), dict_table.values()))
        print(attribut)
        print(valeur)
        end = time.time()
        time1=end-start


        conditionned_att=[]
        valeur_t={}
        vall_t=[]
        attribut_t=[]
        id=[]
        vall = []
        index = []

        def second_loop(args):
            for i in args:
                if attribut[i] == conditions[0]:
                    id.append(i)
                    attribut_t.append(attribut[i])
                    for j in range(len(valeur[i])):
                        if ops[conditions[1]](valeur[i][j], conditions[2]):
                            vall.append(valeur[i][j])
                            index.append(j)
                    vall_t.append(vall)

            '''return index, vall_t, vall, attribut_t'''


        start = time.time()
        print(range(len(attribut)))
        with concurrent.futures.ThreadPoolExecutor() as executor1:
            executor1.submit(second_loop, range(len(attribut)))

        print(index)
        print(vall_t)
        print(vall)
        print(attribut_t)
        end = time.time()
        time2=end-start
        vall1 = []

        def third_loop(args):
            for i in args:
                if attribut[i] != conditions[0]:
                    id.append(i)
                    attribut_t.append(attribut[i])
                    vall1 = [valeur[i][j] for j in index]
                    vall_t.append(vall1)



        start = time.time()
        with concurrent.futures.ThreadPoolExecutor() as executor2:
            executor2.submit(third_loop, range(len(attribut)))

        print(index)
        print(vall_t)
        print(vall1)
        print(attribut_t)
        end = time.time()
        time3 = end - start

        att = [x for _, x in sorted(zip(id, attribut_t))]
        val= [x for _, x in sorted(zip(id, vall_t))]

        for i in range(len(att)):
            valeur_t[att[i]] = val[i]
        time_Total=time1+time2+time3
        print(time_Total)

        disk = DISK_STORAGE_COLUMN + 'select_tables.txt'
        with open(disk, 'w') as json_file:
            json.dump(valeur_t, json_file)


    def join(self,you,jointure,join_type,theta=[]):
        dict_table = copy.deepcopy(self.__dict__)
        dict_table1= copy.deepcopy(you.__dict__)
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
        attribut=[]
        valeur=[]
        attribut1=[]
        valeur1=[]
        def first_loop(args):
            for attribute, val in args:
                attribut.append(attribute)
                valeur.append(val)  
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(first_loop,zip(dict_table.keys(), dict_table.values()))
        '''         
        for attribute, val in zip(dict_table.keys(), dict_table.values()):
            attribut.append(attribute)
            valeur.append(val)
        '''
        def second_loop(args):
            for attribute, val in args:
                attribut1.append(attribute)
                valeur1.append(val)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(second_loop,zip(dict_table1.keys(), dict_table1.values()))
        '''  
        for attribute, val in zip(dict_table1.keys(), dict_table1.values()):
            attribut1.append(attribute)
            valeur1.append(val)
        '''
        ## verification if join is possible for natural join
        if join_type=="natural":
            common_att=[]
 
            def third_loop(args,args1):
                for i in args:
                    for j in args1:
                        if attribut[i]==attribut1[j]:
                            common_att.append((attribut[i],i,j))
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.submit(third_loop,range(len(attribut)),range(len(attribut1)))
            '''
            for i in range(len(attribut)):
                for j in range(len(attribut1)):
                    if attribut[i]==attribut1[j]:
                        common_att.append((attribut[i],i,j))
            '''
            def fourth_loop(args,args1):
                for elem in args:
                    if elem[0] not in args1:
                        idx=attribut.index(elem[0])
                        idx1=attribut1.index(elem[0])
                        attribut[idx]+=self.name_table
                        attribut1[idx1]+=you.name_table
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.submit(fourth_loop,common_att,theta)
            '''
            for elem in common_att:
                if elem[0] not in theta:
                    idx=attribut.index(elem[0])
                    idx1=attribut1.index(elem[0])
                    attribut[idx]+=self.name_table
                    attribut1[idx1]+=you.name_table
            '''
            possible=True
            def fifth_loop(args):
                for elem in args:
                    idx1=attribut.index(elem)
                    idx2=attribut1.index(elem)
                    val=valeur[idx1]
                    val1=valeur[idx2]
                    if len(val)!=len(val1):
                        possible=False
                    for v in val:
                        if v not in val1:
                            possible=False
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.submit(fifth_loop,theta)

            '''
            possible=True
            for elem in theta:
                idx1=attribut.index(elem)
                idx2=attribut1.index(elem)
                val=valeur[idx1]
                val1=valeur[idx2]
                if len(val)!=len(val1):
                    possible=False
                for v in val:
                    if v not in val1:
                        possible=False
            '''
            if(possible==False):
                print("natural join is not possible in this case")
            else:
                attribut2=copy.deepcopy(attribut1)
                valeur2=copy.deepcopy(valeur1)
                def sixth_loop(args):
                    for elem in args :
                        vals=valeur[attribut.index(elem)]
                        for v in vals:
                            target_idx=vals.index(v)
                            current_idx=valeur2[attribut2.index(elem)].index(v)
                            for element in valeur2:
                                element[current_idx],element[target_idx]=element[target_idx],element[current_idx]
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(sixth_loop,theta)
                '''
                for elem in theta :
                    vals=valeur[attribut.index(elem)]
                    for v in vals:
                        target_idx=vals.index(v)
                        current_idx=valeur2[attribut2.index(elem)].index(v)
                        for element in valeur2:
                            element[current_idx],element[target_idx]=element[target_idx],element[current_idx]
                '''

                def seventh_loop(args):
                    for elem in args:
                        idx = attribut2.index(elem)
                        del attribut2[idx]
                        del valeur2[idx]
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(seventh_loop,theta)
                '''
                for elem in theta:
                    idx = attribut2.index(elem)
                    del attribut2[idx]
                    del valeur2[idx]
                '''
                attribut+=attribut2
                valeur+=valeur2
                def eigth_loop(args):
                    for key in args:
                        jointure.add_key(key)
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(eigth_loop,attribut)
                '''
                for key in attribut:
                    jointure.add_key(key)
                '''
                def nineth_loop(args):
                    for val in args:
                        jointure.add_values(val)
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(nineth_loop,valeur)
                '''
                for val in valeur:
                    jointure.add_values(val)
                '''
                jointure.dump()
        else:
            #verification
            common_att=[]
            def tenth_loop(args,args1):
                for i in args:
                    for j in args1:
                        if attribut[i]==attribut1[j]:
                            common_att.append((attribut[i],i,j)) 
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.submit(tenth_loop,range(len(attribut)),range(len(attribut1)))        
            '''
            for i in range(len(attribut)):
                for j in range(len(attribut1)):
                    if attribut[i]==attribut1[j]:
                        common_att.append((attribut[i],i,j))
            '''
            def eleventh_loop(args):
                for elem in args:
                    idx=attribut.index(elem[0])
                    idx1=attribut1.index(elem[0])
                    attribut[idx]+=self.name_table
                    attribut1[idx1]+=you.name_table 
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.submit(eleventh_loop,common_att)                
            '''
            for elem in common_att:
                idx=attribut.index(elem[0])
                idx1=attribut1.index(elem[0])
                attribut[idx]+=self.name_table
                attribut1[idx1]+=you.name_table
            '''
            theta0=theta
            if theta0[0] in common_att:
                theta0[0]+=self.name_table
            if theta0[1] in common_att:
                theta0[1]+=you.name_table
            possible=True
            if len(valeur[0])>=len(valeur1[0]):
                vals=valeur[attribut.index(theta0[0])]
                vals1=valeur1[attribut1.index(theta0[1])]
                def twelveth_loop(args,args1):
                    for v in args:
                        if v not in args:
                            possible=False  
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(twelveth_loop,vals1,vals)            
                '''
                for v in vals1:
                    if v not in vals:
                        possible=False
                '''
            else:
                vals=valeur[attribut.index(theta0[0])]
                vals1=valeur1[attribut1.index(theta0[1])]
                def thirteenth_loop(args,args1):
                    for v in args:
                        if v not in args1:
                            possible=False  
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(thirteenth_loop,vals,vals1)                 
                '''
                for v in vals:
                    if v not in vals1:
                        possible=False
                '''
            if possible==False:
                print("theta join is not possible in this case")
            else :
                if len(valeur[0])>=len(valeur1[0]):
                    new_vals=[[] for i in range(len(attribut1))]
                    target_vals=valeur[attribut.index(theta0[0])]
                    def fourteenth_loop(args,args1,args2):
                        for v in args:
                            for j in args1:
                                if valeur1[attribut1.index(theta0[1])][j]==v:
                                    for i in args2:
                                        new_vals[i].append(valeur1[i][j])  
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        executor.submit(fourteenth_loop,target_vals,range(len(valeur1[attribut1.index(theta0[1])])),range(len(new_vals)))                   
                    '''
                    for v in target_vals:
                        for j in range(len(valeur1[attribut1.index(theta0[1])])):
                            if valeur1[attribut1.index(theta0[1])][j]==v:
                                for i in range(len(new_vals)):
                                    new_vals[i].append(valeur1[i][j])
                    '''
                    attribut+=attribut1
                    valeur+=new_vals
                    def fifthteenth_loop(args):
                        for key in args:
                            jointure.add_key(key)
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        executor.submit(fifthteenth_loop,attribut) 
                    '''
                    for key in attribut:
                        jointure.add_key(key)
                    '''
                    def sixteenth_loop(args):
                        for val in args:
                            jointure.add_values(val)
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        executor.submit(sixteenth_loop,valeur) 
                    '''
                    for val in valeur:
                        jointure.add_values(val)
                    '''
                    jointure.dump()
                else:
                    new_vals=[[] for i in range(len(attribut))]
                    target_vals=valeur1[attribut1.index(theta0[1])]
                    def seventeenth_loop(args,args1,args2):
                        for v in args:
                            for j in args1:
                                if valeur[attribut.index(theta0[0])][j]==v:
                                    for i in args2:
                                        new_vals[i].append(valeur[i][j])
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        executor.submit(seventeenth_loop,target_vals,range(len(valeur[attribut.index(theta0[0])])),range(len(new_vals))) 
                    '''
                    for v in target_vals:
                        for j in range(len(valeur[attribut.index(theta0[0])])):
                            if valeur[attribut.index(theta0[0])][j]==v:
                                for i in range(len(new_vals)):
                                    new_vals[i].append(valeur[i][j])
                    ''' 
                    attribut1+=attribut
                    valeur1+=new_vals
                    def eighteenth_loop(args):
                        for key in args:
                            jointure.add_key(key)
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        executor.submit(eighteenth_loop,attribut1)
                    '''
                    for key in attribut1:
                        jointure.add_key(key)
                    '''

                    def nineteenth_loop(args):
                        for val in args:
                            jointure.add_values(val)
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        executor.submit(nineteenth_loop,valeur1)
                    '''
                    for val in valeur1:
                        jointure.add_values(val)
                    '''
                    jointure.dump()
            

        

class jointure_table():
    def __init__(self,keys=[],val=[]):
        self.keys=keys
        self.values=val
        self.disk=DISK_STORAGE_COLUMN+'joint_tables.txt'
    def add_key(self,key):
        self.keys.append(key)
    def add_values(self,val):
        self.values.append(val)
    def dump(self):
        keys_list = self.keys
        values_list = self.values
        zip_iterator = zip(keys_list, values_list)
        a_dictionary = dict(zip_iterator)
        with open(self.disk, 'w') as json_file:
            json.dump(a_dictionary, json_file)
    def show_joint(self):
        dict_j={}
        for i in range(len(self.keys)):
            dict_j[self.keys[i]]=self.values[i]
        print(dict_j)


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
    def show_table(self,max_row=0):
        cell_txt=[]
        cols=list(self.rows[0].__dict__.keys())
        def first_loop(args):
            for r in args:
                dict_r = r.__dict__
                cell_txt.append(list(dict_r.values()))
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(first_loop,self.rows)
        '''
        for r in self.rows:
            dict_r = r.__dict__
            cell_txt.append(list(dict_r.values()))
        '''
        prim_key=self.__dict__["primary_key_name"]
        second_key=self.__dict__["foreign_key_name"]
        fig, (ax1,ax2) = plt.subplots(2) 
        ax1.set_axis_off() 
        ax2.set_axis_off()
        if max_row!=0:
            cell_txt=cell_txt[:max_row]
        cols_res=copy.deepcopy(cols)
        cell_txt_res=copy.deepcopy(cell_txt)
        def second_loop(args,args1,args2):
            for i in args:
                keep=False
                for elem in args1:
                    if elem[i]!="":
                        keep=True
                if keep==False:
                    cols_res.remove(cols[i])
                    for elem in args2:
                        elem.remove("")
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(second_loop,range(len(cols)),cell_txt,cell_txt_res)
        '''
        for i in range(len(cols)):
            keep=False
            for elem in cell_txt:
                if elem[i]!="":
                    keep=True
            if keep==False:
                cols_res.remove(cols[i])
                for elem in cell_txt_res:
                    elem.remove("")
        '''


        table = ax1.table( 
        cellText = cell_txt_res,  
        colLabels = cols_res,   
        colColours =["palegreen"] * len(cols),
        colLoc='center',  
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

        
        ax1.set_title(self.name_table+"_row", 
            fontweight ="bold")
        ax2.set_title("Primary keys and foreign keys", 
            fontweight ="bold")
        plt.show()


    def projection(self,you,attributes=[]):
        values=[]
        cols=list(self.rows[0].__dict__.keys())
        def first_loop(args):
            for r in args:
                dict_r = r.__dict__
                values.append(list(dict_r.values()))
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(first_loop,self.rows)
        '''   
        for r in self.rows:
            dict_r = r.__dict__
            values.append(list(dict_r.values()))
        '''
        adding=[]
        edx=[]
        def second_loop(args):
            for att in args:
                edx.append(cols.index(att))
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(second_loop,attributes)      
        '''
        for att in attributes:
            edx.append(cols.index(att))
        '''
        adding=[]
        def third_loop(args,args1):
            for i in args:
                temp=["" for j in range(len(cols))]
                for elem in args1 :
                    temp[elem]=values[i][elem]
                if temp not in adding:
                    adding.append(temp)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(third_loop,range(len(values)),edx) 
        '''
        for i in range(len(values)):
            temp=["" for j in range(len(cols))]
            for elem in edx :
                temp[elem]=values[i][elem]
            if temp not in adding:
                adding.append(temp)
        '''
        def fourth_loop(args):
            for elem in args:
                you.add(elem)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(fourth_loop,adding)    
        '''
        for elem in adding:
            you.add(elem)
        '''
        locname=""
        for att in attributes:
            locname+=att+"_"
        you.disk = DISK_STORAGE_ROW + self.name_table + '_projection_'+locname+'row.txt'
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
        list_row_t=[]
        values = []
        attribut = []

        def first_loop(rows):
            for data_row in rows:
                for attribute in data_row.keys():
                    if (attribute == conditions[0] and ops[conditions[1]](data_row[attribute], conditions[2])):
                        list_row_t.append(data_row)
                        values.append(data_row[attribute])

        start = time.time()
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(first_loop, rows)
        end = time.time()
        time3 = end - start
        print(time3)
        disk = DISK_STORAGE_ROW + 'select_tables.txt'
        with open(disk, 'w') as json_file:
            json.dump(list_row_t, json_file)
    def join_row(self, you, join_type, theta=[]):

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
        rows=  dict_dump['rows']

        list_row1 = []
        for r in you.rows:
            dict_r = r.__dict__
            list_row1.append(dict_r)

        dict_dump1 = copy.deepcopy(you.__dict__)
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
            List_rows=[]
            def first_loop(args):
                rows=args[0]
                rows1=args[1]
                if len(rows) >= len(rows1):
                    print(List_rows)
                    for i in range(len(rows)):
                        for k in range(len(rows1)):
                            if rows[i][theta[0]]==rows1[k][theta[1]]:
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
                            if rows1[i][theta[1]]==rows[k][theta[0]]:
                                for n in range(len(list(rows[k].keys()))):
                                    if list(rows[k].keys())[n] in attribut1:
                                        rows1[i][list(rows[k].keys())[n] + '1'] = rows[k][list(rows[k].keys())[n]]
                                    else:
                                        rows1[i][list(rows[k].keys())[n]] = rows[k][list(rows[k].keys())[n]]
                    List_rows.append(rows1)

            start = time.time()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.submit(first_loop, [rows, rows1])
            end = time.time()
            time3 = end - start
            print(time3)
            print(List_rows)
            disk = DISK_STORAGE_ROW + 'join_tables_theta.txt'
            with open(disk, 'w') as json_file:
                json.dump(List_rows, json_file)



        if join_type == "natural":
            attribut=[]
            attribut1=[]
            for data_row in rows[0]:
                attribut.append(data_row)
            for data_row1 in rows1[0]:
                attribut1.append(data_row1)

            print(attribut)
            print(attribut1)

            possible=True
            for elem in theta:
                if (elem not in attribut) or (elem not in attribut1):
                    print('The databases does not have a column in commun')
                    possible=False
                else:
                    possible=True

            if possible == True:
                index=[]
                index1=[]
                def second_loop(args):
                    rows=args[0]
                    rows1=args[1]
                    for i in range(len(rows)):
                        for j in range(len(rows[i].keys())):
                            Keys=list(rows[i].keys())
                            for k in range(len(rows1)):
                                for l in range(len(rows1[k].keys())):
                                        Keys1=list(rows1[k].keys())
                                        if Keys[j] in theta and Keys1[l] in theta:
                                            if rows[i][list(rows[i].keys())[j]] == rows1[k][list(rows1[k].keys())[l]]:
                                                index.append([i,j])
                                                index1.append([k,l])

                start = time.time()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(second_loop, [rows, rows1])
                end = time.time()
                time3 = end - start
                print(time3)

                List_rows=[]
                def third_loop(args):
                    index=args[0]
                    index1=args[1]
                    for i,j in index:
                        valeur_t = {}
                        valeur_t[list(rows[i].keys())[j]]=rows[i][list(rows[i].keys())[j]]
                        for m in range(len(rows[i].keys())):
                            if m!=j:
                                valeur_t[list(rows[i].keys())[m]] = rows[i][list(rows[i].keys())[m]]
                        List_rows.append(valeur_t)

                    s = 0
                    for k, l in index1:
                        print(List_rows[s])
                        for n in range(len(rows1[k].keys())):
                            if n != l:
                                if list(rows1[k].keys())[n] in attribut:
                                    List_rows[s][list(rows1[k].keys())[n]+'1'] = rows1[k][list(rows1[k].keys())[n]]
                                else:
                                    List_rows[s][list(rows1[k].keys())[n]] = rows1[k][list(rows1[k].keys())[n]]

                        s+=1

                start = time.time()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    executor.submit(third_loop, [index, index1])
                end = time.time()
                time3 = end - start
                print(time3)

                print(List_rows)
                disk = DISK_STORAGE_ROW + 'join_tables.txt'
                with open(disk, 'w') as json_file:
                    json.dump(List_rows, json_file)



class jointure_table_row():
    def __init__(self):
        self.disk=DISK_STORAGE_ROW+'joint_tables.txt'
        self.rows = []

    def add_rows(self,row):
        self.rows.append(row)

    def dump_row(self):
        list_row=[]
        for r in self.rows:
            print(r)
            dict_r = r
            list_row.append(dict_r)
        data={}
        data['rows']=list_row
        print(data)
        with open(self.disk, 'w') as json_file:
            json.dump(data, json_file)
        


