
import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import types
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql.functions import *

dir_path = "/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/sample/School/"

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


def joinTwo(dir_path,primary_key):
    df_list = create_dfs(dir_path=dir_path)
    df1, df2 = df_list[0], df_list[1]
    df_total = df1.join(df2, on= primary_key)
    return df_total

def unionCheck(dir_path, primary_key):
    from functools import reduce
    df_list = create_dfs(dir_path=dir_path)
    df_res = df_list[0]
    for df_next in df_list[1:]:
        df_res = df_res.join(df_next, on= primary_key, how= 'outer')
    return df_res

sql_resource_payload = "SELECT " \
                       "to_json(map('schoolId',school_data.schoolId)) AS PrimaryKey," \
                       "to_json(map(" \
                       "'educationOrganizationCategories', to_json(map('educationOrganizationCategoryDescriptor',school_data.educationOrganizationCategoryDescriptor))," \
                       "'gradeLevels', to_json(map('gradeLevelDescriptor', school_data.gradeLevelDescriptor))," \
                       "'schoolId', school_data.schoolId," \
                       "'charterApprovalSchoolYearTypeReference', to_json(map('schoolYear', school_data.charterApprovalSchoolYearTypeReference_schoolYear))," \
                       "'localEducationAgencyReference', to_json(map('localEducationAgencyId', school_data.localEducationAgencyId))" \
                       "))" \
                       "AS Payload " \
                       "FROM " \
                       "school_data"






if __name__ == "__main__":

    data = unionAll(dir_path, primary_key= ["schoolId", "Operation"])
    data.createOrReplaceTempView("school_data")
    """Ref: https://stackoverflow.com/questions/60435907/pyspark-merge-multiple-columns-into-a-json-column/60436292#60436292"""
    cols = data.columns

    # .withColumn('addresses',to_json(struct('addressTypeDescriptor')))\

    from pyspark.sql import functions as F, types as T

    def build_schema():
        schema = StructType([
        StructField("schoolId", StringType()),
        StructField("primaryKey", StringType(),IntegerType()),
        StructField("payload",StructType(
            StructField("schoolId", IntegerType(), nullable= False),


            """Refer this : http://nadbordrozd.github.io/blog/2016/05/22/one-weird-trick-that-will-fix-your-pyspark-schemas/"""
        ) )
    ])
        return schema



    df_spark = data.withColumn('educationOrganizationCategories',array(to_json(struct('educationOrganizationCategoryDescriptor'))))\
        .withColumn('gradeLevels', array(to_json(struct("gradeLevelDescriptor"))))\
        .withColumn('charterApprovalSchoolYearTypeReference', to_json(struct('charterApprovalSchoolYearTypeReference_schoolYear')))\
        .withColumn('localEducationAgencyReference', to_json(struct('localEducationAgencyId')))\
        .withColumn('identificationCodes', to_json(struct('educationOrganizationIdentificationSystemDescriptor', 'identificationCode')))\
        .withColumn('indicators', to_json(array(struct('indicatorDescriptor','indicatorGroupDescriptor','indicatorLevelDescriptor', 'designatedBy','indicatorValue')))) \
        .withColumn('institutionTelephones', to_json(struct('institutionTelephoneNumberTypeDescriptor', 'telephoneNumber'))) \
        .withColumn('schoolCategories', to_json(struct(data.schoolCategoryDescriptor))) \
        .select('schoolId',
                to_json(struct("schoolId")).alias('primaryKey'),
                to_json(struct(data.schoolId,
                               "educationOrganizationCategories",
                               "gradeLevels",
                               "charterApprovalSchoolYearTypeReference",
                               "localEducationAgencyReference",
                               'identificationCodes',
                               'indicators',
                               "institutionTelephones",
                               "schoolCategories"
                               )).alias('payload'))


    print(df_spark.show(truncate=False))
    import json
    # from pyspark.sql import types as T
    #
    # # def to_json(data):
    # #     return json.dumps({'data': data})
    #
    #
    # to_json_udf = udf(to_json, T.StringType())
    # df_payloads = df_spark.select(to_json_udf('payload'))
    # print(df_payloads.show(truncate=False))

    # print(df_spark.show(truncate=False))
    # df_spark = df_spark.filter(df_spark.)


    # print(df_spark.select(df_spark.payload))
    # schema = df_spark.schema
    # schema_json = schema.json()

    # from pyspark.sql.types import DataType, StructType
    # import json
    #
    # new_schema = StructType.fromJson(json.loads(schema_json))
    # print(new_schema)

    # print(df.show(truncate=False))

"""to list: https://stackoverflow.com/questions/53477724/pyspark-how-to-create-a-nested-json-from-spark-data-frame"""







