import pyspark
from pyspark.shell import sqlContext
from pyspark.sql.types import StringType,StructType,StructField,BooleanType, ArrayType, IntegerType
from pyspark.sql import SparkSession

# def school_base_schema():
#     schema_school_base = StructType([
#     StructField("Operation", StringType(), nullable=False),
#     StructField("schoolId",IntegerType(), nullable=False),
#     StructField("administrativeFundingControlDescriptor", StringType(), nullable=True),
#     StructField("charterApprovalSchoolYearTypeReference_schoolYear", StringType(), nullable=False),
#     StructField("localEducationAgencyId", IntegerType(), nullable=False),
#     StructField("schoolYearTypeReference_schoolYear", StringType(), nullable=False),
#     StructField("charterStatusDescriptor", StringType(), nullable=False),
#     StructField("internetAccessDescriptor", StringType(), nullable= False),
#     StructField("magnetSpecialProgramEmphasisSchoolDescriptor", StringType(), nullable= False),
#     StructField("nameOfInstitution", StringType()),
#     StructField("operationalStatusDescriptor", StringType()),
#     StructField("schoolTypeDescriptor", StringType()),
#     StructField("shortNameOfInstitution", StringType()),
#     StructField("titleIPartASchoolDesignationDescriptor", StringType()),
#     StructField("webSite", StringType()),
#     StructField("tx_PKFullDayWaiver", StringType()),
#     StructField("tx_AdditionalDaysProgram", StringType()),
#     StructField("tx_NumberOfBullyingIncidents", BooleanType()),
#     StructField("tx_NumberOfCyberbullyingIncidentsc", IntegerType())
#     ])
#     return schema_school_base

dir_path = "/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/sample/School/"

def sparkSessionBuilder():
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
    return spark

def spark_dataframe(path):
    spark = sparkSessionBuilder()
    df = spark.read.format('csv').options(Header=True).option('inferSchema','True').load(path)
    return df

def set_df_columns_nullable(spark, df, column_list, nullable=True):
    for struct_field in df.schema:
        if struct_field.name in column_list:
            struct_field.nullable = nullable
    df_mod = spark.createDataFrame(df.rdd, df.schema)
    return df_mod

col_list = ["schoolId","nameOfInstitution"]

spark = sparkSessionBuilder()

df_school_base = spark_dataframe(dir_path + "School_Base.csv")

df_mod = set_df_columns_nullable(spark=spark, df = df_school_base, column_list=col_list, nullable=False)

