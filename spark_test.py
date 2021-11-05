import os
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType, DateType
from pyspark.sql.functions import sum, col, desc, asc

spark = SparkSession.builder.appName('TestSpark').getOrCreate()


def spark_group_row(filename, group_by):

    file_path = os.path.join(os.getcwd(), 'disk_storage_row\\ ' + filename)
    with open(file_path, 'r') as json_file:
        table = json.load(json_file)

    df = spark.createDataFrame(table['rows'])

    df = df.withColumn("commitdate", df.discount.cast(DateType()))
    df = df.withColumn("discount", df.discount.cast(FloatType()))
    df = df.withColumn("extendedprice", df.extendedprice.cast(FloatType()))
    df = df.withColumn("quantity", df.quantity.cast(FloatType()))
    df = df.withColumn("receiptdate", df.receiptdate.cast(DateType()))
    df = df.withColumn("returnflag", df.returnflag.cast(FloatType()))
    df = df.withColumn("shipinstruct", df.shipinstruct.cast(DateType()))

    start = time.time()
    df_grouped = df.groupBy(group_by).sum().sort(desc("sum(quantity)"))
    duration = time.time() - start
    print(f'Time spent for group by on rows storage: {duration}')
    df_grouped.show()
    return df_grouped


# TODO - def group by on column storage
'''filename = 'LINEITEM_column.txt'
file_path = os.path.join(os.getcwd(), 'disk_storage_column\\ ' + filename)
with open(file_path, 'r') as json_file:
    table = json.load(json_file)

#print(table.keys())
del table['primary_key_name']
del table['foreign_key_name']
#df = spark.createDataFrame(table)
df = spark.read.json(file_path)
df.show()'''


def spark_sum_row(filename, col_to_sum):

    file_path = os.path.join(os.getcwd(), 'disk_storage_row\\ ' + filename)
    with open(file_path, 'r') as json_file:
        table = json.load(json_file)

    df = spark.createDataFrame(table['rows'])
    start = time.time()
    df_sum = df.agg({col_to_sum: 'sum'})
    duration = time.time() - start
    df_sum.show()
    print(f'Time spent for group by on rows storage: {duration}')

#spark_sum_row('LINEITEM_row.txt', 'quantity')
#spark_group_row('LINEITEM_row.txt', 'suppkey')