import json
import pyspark
from pyspark.shell import sqlContext
from pyspark.sql.functions import to_json, struct, col, array, when
from pyspark.sql.types import StringType, StructType, StructField, BooleanType, ArrayType, IntegerType, MapType
from pyspark.sql import SparkSession
import json
from json import JSONDecodeError
from time import time

# dir_path = "/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/from_customer/innive_data/ssa/"

dir_path = "/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/sample/SSA/"

start = time()


def sparkSessionBuilder():
    spark = SparkSession.builder.master("local[3]").appName("test") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.ui.port", "4050") \
        .getOrCreate()
    return spark


def spark_dataframe(path):
    spark = sparkSessionBuilder()
    df = spark.read.format('csv') \
        .options(Header=True) \
        .option('inferSchema', 'True') \
        .load(path)

    print("number of partitions: ", df.rdd.getNumPartitions())
    df.repartition(4)

    # df = df.coalesce(2)
    print("number of new partitions: ", df.rdd.getNumPartitions())
    return df


def unionAll(dfs, primary_key):
    from functools import reduce
    new_data = reduce(lambda df1, df2: df1.join(df2, primary_key, how='outer'), dfs)
    return new_data


def unionCheck(dfs, primary_key):
    from functools import reduce
    df_list = dfs
    df_res = df_list[0]
    for df_next in df_list[1:]:
        df_res = df_res.join(df_next, on=primary_key, how='outer')
    return df_res


if __name__ == "__main__":
    spark = sparkSessionBuilder()

    def transform_ssa_base(filepath="SSA_Base.csv"):
        df_base = spark_dataframe(path=dir_path + filepath)
        df_base = df_base \
            .withColumn("schoolReference", to_json(struct("schoolRef_schoolId"))) \
            .withColumn("studentReference", to_json(struct("studentUniqueId"))) \
            .withColumn("calendarReference",
                        to_json(struct("calendarCode", "calendarReference_schoolYear", "calendarReference_schoolId"))) \
            .withColumn("graduationPlanReference",
                        to_json(struct([str(c) for c in df_base.columns if c not in {'studentUniqueId', 'Operation'}])))
        return df_base


    def transform_ssa_agpr(filepath="SSA_alternateGraduationPlanReference.csv"):
        df_agpr = spark_dataframe(path=dir_path + filepath)
        df_agpr = df_agpr.withColumn("alternativeGraduationPlans",
                                     array(to_json(struct([str(c) for c in df_agpr.columns if
                                                           c not in {'studentUniqueId', 'Operation'}]))))
        return df_agpr

    def transform_ssa_eduPlans(filepath="SSA_educationPlans.csv"):
        df_base = spark_dataframe(path=dir_path + filepath)
        df_base = df_base.withColumn("educationPlans", to_json(struct("educationPlanDescriptor")))
        return df_base

    def get_joined_data():
        primary_key = ["studentUniqueId", "Operation", "schoolRef_schoolId"]
        df_ssaBase = transform_ssa_base(filepath="SSA_Base.csv")
        df_ssaAGPR = transform_ssa_agpr(filepath="SSA_alternateGraduationPlanReference.csv")
        # df_ssaEduPlans = transform_ssa_eduPlans(filepath="SSA_educationPlans.csv")
        # dfs = [df_ssaBase, df_ssaAGPR, df_ssaEduPlans]
        dfs = [df_ssaBase, df_ssaAGPR]
        df_student = unionAll(dfs=dfs, primary_key=primary_key)
        df_student = df_student.select("studentUniqueId", "Operation",
                                       to_json(struct([str(c) for c in df_student.columns
                                        if c not in {'Operation', 'studentUniqueId', 'schoolId'}])).alias(
                                           "payload"))

        # df_student.write.mode("overwrite").csv(
        #     "/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/from_customer/dataOutput",
        #     header=True)
        return df_student


    df_student = get_joined_data()
    df_student.show(truncate=False)

    end = time()
    total = end - start
    print(total)

    input("Press enter to terminate")

    spark.stop()
