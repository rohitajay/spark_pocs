import ast
import json
import pyspark
from pyspark.shell import sqlContext
import pyspark.sql.functions as F
from pyspark.sql.functions import to_json, struct, col, array, when, regexp_extract
from pyspark.sql.types import StringType, StructType, StructField, BooleanType, ArrayType, IntegerType, MapType
from pyspark.sql import SparkSession
import json
from json import JSONDecodeError
from student_schema import *
from time import time
from pyspark.sql import SparkSession
import findspark
# add the respective path to your spark
findspark.init('_path-to-spark_')


start = time()
# dir_path = "/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/sample/Student/"
dir_path = "/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/from_customer/innive_data/student/"



def sparkSessionBuilder(url= "localhost" ,port = "4050",log_level= "ALL"):
    spark = SparkSession\
        .builder\
        .master("local") \
        .config("spark.driver.bindAddress", url) \
        .config("spark.ui.port", port, ) \
        .appName("test").getOrCreate()

    # sc = spark.sparkContext.setLogLevel("ALL")
    """https://polarpersonal.medium.com/writing-pyspark-logs-in-apache-spark-and-databricks-8590c28d1d51"""
    """https://medium.com/@shantanualshi/logging-in-pyspark-36b0bd4dec55"""
    return spark


def spark_dataframe(path,created_schema):
    spark = sparkSessionBuilder(url= "localhost" ,port = "4050")
    # sc = spark.sparkContext(conf="log4j.properties")
    # spark.sparkContext(conf="log4j.properties").setLogLevel("ALL")
    # df = spark.read.format('csv').options(Header=True).option('inferSchema','True').load(path)  (slower due to infer schema)
    # https://stackoverflow.com/questions/56895707/pyspark-difference-performance-for-spark-read-formatcsv-vs-spark-read-csv
    df = spark.read.csv(path, header=True, nullValue='NA', schema=created_schema)
    return df

def unionAll(dfs, primary_key):
    from functools import reduce
    new_data = reduce(lambda df1, df2: df1.join(df2, primary_key, how='inner'), dfs)
    return new_data


def unionCheck(dfs, primary_key):
    from functools import reduce
    df_list = dfs
    df_res = df_list[0]
    for df_next in df_list[1:]:
        df_res = df_res.join(df_next, on=primary_key, how='inner')
    return df_res


def remove_empty_elements(d):
    """recursively remove empty lists, empty dicts, or None elements from a dictionary
    ref: https://gist.github.com/nlohmann/c899442d8126917946580e7f84bf7ee7
    """

    def empty(x):
        return x is None or x == {} or x == []

    if not isinstance(d, (dict, list)):
        return d
    elif isinstance(d, list):
        return [v for v in (remove_empty_elements(v) for v in d) if not empty(v)]
    else:
        return {k: v for k, v in ((k, remove_empty_elements(v)) for k, v in d.items()) if not empty(v)}

def drop_null_columns(df, min_none=0):
    """
    This function drops all columns which contain null values.
    :param df: A PySpark DataFrame
    """
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[
        0].asDict()
    to_drop = [k for k, v in null_counts.items() if v > min_none]
    df = df.drop(*to_drop)
    return df.columns


if __name__ == "__main__":


    def transform_student_base(filepath= "Student_Base.csv", created_schema = None):
        df_base = spark_dataframe(path=dir_path + filepath, created_schema=created_schema)
        # columns_with_value = drop_null_columns(df_base, min_none=0)
        df_base = df_base.withColumn("personReference", to_json(struct("personId", "sourceSystemDescriptor"))).drop("personId", "sourceSystemDescriptor")
        return df_base

    def transform_student_IdDocs(filepath = "Student_identificationDocuments.csv", created_schema = None):
        df_studentIdDocs = spark_dataframe(path=dir_path + filepath, created_schema=created_schema)
        df_studentIdDocs = df_studentIdDocs.withColumn("personalIdentificationDocuments",
                                                       array(to_json(struct([str(c) for c in df_studentIdDocs.columns if c not in {'studentUniqueId','Operation'}]))))

        return df_studentIdDocs

    def transform_student_otherNames(filepath = "Student_otherNames.csv", created_schema = None):
        df_otherNames = spark_dataframe(path=dir_path + filepath, created_schema=created_schema)
        print("abc---->", [col(c) for c in df_otherNames.columns])
        columns_with_value = drop_null_columns(df_otherNames, min_none=0)
        columns_with_value = [str(c) for c in columns_with_value if c not in {'Operation', 'studentUniqueId' }]

        print(columns_with_value)

        if len(columns_with_value) > 0:
            df_otherNames = df_otherNames \
                .withColumn("otherNames", array(to_json(struct(
                columns_with_value
            ))))
        else:
            return None

        # with_null_columns = [str(c) for c in df_otherNames.columns
        #                                       if c not in {'Operation', 'studentUniqueId' }]
        #
        #
        # without_null_columns = [when(col(c).isNotNull(), col(c)).otherwise(F.lit("")).alias(c) for c in df_otherNames.columns if c not in {'studentUniqueId','Operation'}]
        #
        # df_otherNames = df_otherNames\
        #     .withColumn("otherNames", array(to_json(struct(
        #     without_null_columns
        # ))))
        # print(df_otherNames.columns)
        # return df_otherNames

    def transform_student_pid(filepath = "Student_personalidentificationDocuments.csv", created_schema = None):
        df_pid = spark_dataframe(path=dir_path + filepath, created_schema=created_schema)

        without_null_columns = [when(col(c).isNotNull(), col(c)).otherwise(F.lit("")).alias(c) for c in df_pid.columns if c not in {'studentUniqueId','Operation'}]
        columns_with_value = drop_null_columns(df_pid, min_none=0)
        columns_with_value = [str(c) for c in columns_with_value if c not in {'Operation', 'studentUniqueId'}]
        # with_null_columns = [str(c) for c in df_pid.columns if c not in {'studentUniqueId','Operation'}]
        if len(columns_with_value) > 0:
            df_pid = df_pid\
            .withColumn("personalIdentificationDocuments", array(to_json(struct(columns_with_value))))\

            return df_pid
        else:
            return None



    def transform_scbg(filepath = "Student_studentCensusBlockGroups.csv", created_schema=None):
        df_scbg = spark_dataframe(path=dir_path + filepath, created_schema=created_schema)

        pass

    def transform_crisisEvents(filepath = "Student_CrisisEvents.csv", created_schema=None):
        df_crisisEvents = spark_dataframe(path=dir_path + filepath, created_schema=created_schema)

        pass

    def transform_student_visa(filepath = "Student_visas.csv", created_schema=None):
        df_visa = spark_dataframe(path=dir_path + filepath, created_schema=created_schema)
        columns_with_value = drop_null_columns(df_visa, min_none=0)
        columns_with_value = [str(c) for c in columns_with_value if c not in {'Operation', 'studentUniqueId'}]
        if len(columns_with_value) > 0:
            df_visa = df_visa.withColumn("visas", array(to_json(struct(columns_with_value))))
            return df_visa
        else:
            return None




    def get_joined_data():

        primary_key = ["studentUniqueId","Operation"]

        df_base = transform_student_base(filepath= "Student_Base.csv",created_schema=schema_student_base())
        df_visa = transform_student_visa(filepath = "Student_visas.csv",created_schema=schema_student_visas())
        df_pid = transform_student_pid(filepath="Student_personalidentificationDocuments.csv", created_schema=schema_student_pid())
        df_otherNames = transform_student_otherNames(filepath = "Student_otherNames.csv",created_schema=schema_student_otherNames())

        dfs = [df_base, df_visa, df_pid, df_otherNames ]
        dfs = [x for x in dfs if x is not None]
        df_student = unionAll(dfs=dfs, primary_key = primary_key)


        df_student = df_student.withColumn('payload',to_json(
                                         struct(
                                             [str(c) for c in df_student.columns
                                              if c not in {'Operation', 'visaDescriptor', 'firstName',
                                                           'generationCodeSuffix', 'lastSurname',
                                                           'personalTitlePrefix',
                                                           'middleName', 'otherNameTypeDescriptor' }])))

        not_null_columns = [when(col(x).isNotNull(), col(x)).otherwise(F.lit("")).alias(x) for x in df_student.columns if
                            x not in {'Operation', 'visaDescriptor', 'firstName',
                                                           'generationCodeSuffix', 'lastSurname',
                                                           'personalTitlePrefix',
                                                           'middleName' }]
        # df_student = df_student.select("studentUniqueId", "Operation",
        #                              to_json(
        #                                  struct(
        #                                      [str(c) for c in df_student.columns
        #                                       if c not in {'Operation', 'visaDescriptor', 'firstName',
        #                                                    'generationCodeSuffix', 'lastSurname',
        #                                                    'personalTitlePrefix',
        #                                                    'middleName', 'otherNameTypeDescriptor' }])).alias("payload"))



        # update_app_data_udf = F.udf(lambda x: remove_empty_elements(ast.literal_eval(x)),MapType(StringType(),StringType()))
        # df_student = df_student.withColumn("new_column", update_app_data_udf(F.col("payload")))


        return df_student

    #
    # df = transform_student_visa(filepath = "Student_visas.csv")
    # df.show(truncate=False)
    from pprint import pprint
    df_student = get_joined_data()

    df_student.show(truncate= False)

    end = time()

    total = end - start
    print(total)
    # a = transform_student_IdDocs(filepath = "Student_identificationDocuments.csv")
    # print(a.show(truncate=False))