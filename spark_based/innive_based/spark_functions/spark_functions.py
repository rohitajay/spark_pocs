import json

import pyspark
from pyspark.shell import sqlContext
from pyspark.sql.functions import to_json, struct, col, array
from pyspark.sql.types import StringType, StructType, StructField, BooleanType, ArrayType, IntegerType, MapType
from pyspark.sql import SparkSession


def sparkSessionBuilder():
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
    return spark

def spark_dataframe(path):
    spark = sparkSessionBuilder()
    df = spark.read.format('csv').options(Header=True).option('inferSchema','True').load(path)
    return df

def get_file_list(dir_path):
    import glob, os
    file_list = []
    os.chdir(dir_path)
    for file in glob.glob("*.csv"):
        file_path = dir_path + file
        file_list.append(str(file_path))
    return file_list



def create_dfs(dir_path):
    file_list = get_file_list(dir_path=dir_path)
    # print("======================>",file_list)
    dfs = [spark_dataframe(file) for file in file_list]
    return dfs


def unionAll(dir_path, primary_key):
    from functools import reduce
    dfs = create_dfs(dir_path=dir_path)
    new_data = reduce(lambda df1,df2: df1.join(df2, primary_key, how='outer'), dfs)
    return new_data
