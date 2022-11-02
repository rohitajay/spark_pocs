import json
import pyspark
from pyspark.shell import sqlContext
from pyspark.sql.functions import to_json, struct, col, array, when
from pyspark.sql.types import StringType, StructType, StructField, BooleanType, ArrayType, IntegerType, MapType
from pyspark.sql import SparkSession
import json
from json import JSONDecodeError

def sparkSessionBuilder():
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
    return spark

def spark_dataframe(path):
    spark = sparkSessionBuilder()
    df = spark.read.format('csv').options(Header=True).option('inferSchema','True').load(path)
    return df


dir_path = "/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/sample/Student/"

if __name__ == "__main__":

    def transform_student_base(filepath= "Student_Base.csv"):
        df_base = spark_dataframe(path=dir_path + filepath)
        df_base = df_base.withColumn("personReference", to_json(struct("personId", "sourceSystemDescriptor")))
        return df_base

    def transform_student_IdDocs(filepath = "Student_identificationDocuments.csv"):
        df_studentIdDocs = spark_dataframe(path=dir_path + filepath)
        df_studentIdDocs = df_studentIdDocs.withColumn("personalIdentificationDocuments",
                                                       array(to_json(struct([str(c) for c in df_studentIdDocs.columns if c not in {'studentUniqueId','Operation'}]))))

        # df_studentIdDocs = df_studentIdDocs.selectExpr("explode(personalIdentificationDocuments)").withColumn("personalIdentificationDocuments",
        #                                                when(col("personalIdentificationDocuments") == "", None)
        #                                                .otherwise(col("personalIdentificationDocuments")))
        return df_studentIdDocs

    def transform_student_otherNames(filepath = "Student_otherNames.csv"):
        df_otherNames = spark_dataframe(path=dir_path + filepath)
        print(df_otherNames.columns)
        df_otherNames = df_otherNames\
            .withColumn("otherNames", array(to_json(struct(
            [c for c in df_otherNames.columns if c not in {'studentUniqueId','Operation'}]
        )))).select("studentUniqueId","Operation","otherNames")
        return df_otherNames

    def transform_student_pid(filepath = "Student_personalidentificationDocuments.csv"):
        df_pid = spark_dataframe(path=dir_path + filepath)
        df_pid = df_pid.withColumn("personalIdentificationDocuments", array(to_json(struct([str(c) for c in df_pid.columns if c not in {'studentUniqueId','Operation'}]))))
        return df_pid


    def transform_scbg(filepath = "Student_studentCensusBlockGroups.csv"):
        df_scbg = spark_dataframe(path=dir_path + filepath)

        pass

    def transform_crisisEvents(filepath = "Student_CrisisEvents.csv"):
        df_crisisEvents = spark_dataframe(path=dir_path + filepath)

        pass

    def transform_student_visa(filepath = "Student_visas.csv"):
        df_visa = spark_dataframe(path=dir_path + filepath)
        df_visa = df_visa.withColumn("visas", array(to_json(struct([str(c) for c in df_visa.columns if c not in {'studentUniqueId','Operation'}]))))
        return df_visa


    def unionAll(dfs, primary_key):
        from functools import reduce
        new_data = reduce(lambda df1, df2: df1.join(df2, primary_key, how='outer'), dfs)
        return new_data


    def get_joined_data():

        primary_key = ["studentUniqueId","Operation"]

        df_base = transform_student_base(filepath= "Student_Base.csv")
        df_visa = transform_student_visa(filepath = "Student_visas.csv")
        df_pid = transform_student_pid(filepath="Student_personalidentificationDocuments.csv")
        df_otherNames = transform_student_otherNames(filepath = "Student_otherNames.csv")

        dfs = [df_base, df_visa, df_pid, df_otherNames ]
        df_student = unionAll(dfs=dfs, primary_key = primary_key)
        df_student.show(truncate= False)
        df_student = df_student.select("studentUniqueId", "Operation",
                                     to_json(
                                         struct(
                                             [str(c) for c in df_student.columns
                                              if c not in {'Operation', 'visaDescriptor' }])).alias("payload"))
        return df_student

    #
    # df = transform_student_visa(filepath = "Student_visas.csv")
    # df.show(truncate=False)

    df_student = get_joined_data()
    df_student.show(truncate= False)

    # a = transform_student_IdDocs(filepath = "Student_identificationDocuments.csv")
    # print(a.show(truncate=False))