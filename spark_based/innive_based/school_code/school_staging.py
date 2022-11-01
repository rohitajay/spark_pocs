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


dir_path = "/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/sample/School/"

if __name__ == "__main__":
    abc = "schoolYearTypeReference_schoolYear"
    spark = sparkSessionBuilder()
    df_base = spark_dataframe(path= dir_path + "School_Base.csv")


    base_columns = [str(c) for c in df_base.columns if c not in {"Operation","charterApprovalSchoolYearTypeReference_schoolYear"}]
    df_base = df_base\
        .withColumn("charterApprovalSchoolYearTypeReference", to_json(struct(df_base.charterApprovalSchoolYearTypeReference_schoolYear.alias("schoolYear"))))\
        .withColumn("localEducationAgencyReference", to_json(struct(df_base.localEducationAgencyId)))\

    # print(df_base.show(truncate=False))

    df_addresses = spark_dataframe(path= dir_path + "School_addresses.csv")
    df_addresses = df_addresses.select("schoolId","Operation",array(to_json(struct([str(c) for c in df_addresses.columns if c not in {'schoolId','Operation'}]))).alias("addresses"))

    # df_addresses.show(truncate=False)
    #
    df_gradeLevels = spark_dataframe(path= dir_path + "School_gradeLevels.csv")
    df_gradeLevels = df_gradeLevels.select("schoolId","Operation", to_json(struct(df_gradeLevels.gradeLevelDescriptor)).alias("gradeLevels"))

    df_gradeLevels.show(truncate=False)

    df_school = df_base\
        .join(df_gradeLevels, on= ["schoolId","Operation"])\
        .join(df_addresses, on= ["schoolId","Operation"])\
        # .join(df_eoc, on = ["schoolId","Operation"])
    #

    df_school = df_school.select("schoolId",to_json(struct(df_school.drop("Operation","charterApprovalSchoolYearTypeReference_schoolYear")["*"],"addresses", "gradeLevels" )).alias("payload"))
    df_school.show(truncate=False)


    from pprint import pprint
    col_payload = df_school.select(df_school.payload).collect()
    list1 = [json.loads(i[0]) for i in col_payload]
    print(list1)
