from TPC_H_structure.customer import Customer_column, Customer_row
from TPC_H_structure.nation import Nation_column, Nation_row
from table_multithread import Table_column, jointure_table
import time
DISK_STORAGE_COLUMN = '/home/romin/Documents/M2 Data Science/Systems for big data ' \
                      'analytics/INF670E-Project/disk_storage_column/ '
DISK_STORAGE_ROW = '/home/romin/Documents/M2 Data Science/Systems for big data ' \
                   'analytics/INF670E-Project/disk_storage_row/ '
customer = Customer_column() 
nation = Nation_column() 
jointure = jointure_table()

customer.add(['1', 'Customer#000000001', 'IVhzIApeRb ot,c,E', '15', '25-989-741-2988', '711.56', 'BUILDING', 'to the even, regular platelets. regular, ironic epitaphs nag e']) 
customer.add(['0', 'Customer#000000002', 'XSTf4,NCwDVaWNe6tEgvwfmRchLXak', '13', '23-768-687-3665', '121.65', 'AUTOMOBILE', 'l accounts. blithely ironic theodolites integrate boldly: caref']) 
customer.add(['3', 'Customer#000000003', 'MG9kdTD2WBHm', '1', '11-719-748-3364', '7498.12', 'AUTOMOBILE', ' deposits eat slyly ironic, even instructions. express foxes detect slyly. blithely even accounts abov']) 

nation.add(['0', 'Customer#000000002', '0', ' haggle. carefully final deposits detect slyly agai']) 
nation.add(['1', 'Customer#000000001', '1', 'al foxes promise slyly according to the regular accounts. bold requests alon']) 
nation.add(['2', 'Customer#000000003', '1', 'y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special ']) 

customer.disk=DISK_STORAGE_COLUMN + customer.name_table + '_join_'+'column.txt'
nation.disk=DISK_STORAGE_COLUMN + nation.name_table + '_join_'+'column.txt'

customer.dump_column()
nation.dump_column()
t=time.time()
customer.join(nation,jointure,'natural',['name'])
print(time.time()-t)
jointure.show_joint()




