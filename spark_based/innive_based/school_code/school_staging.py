import json
import pyspark
from pyspark.shell import sqlContext
from pyspark.sql.functions import to_json, struct, col, array
from pyspark.sql.types import StringType, StructType, StructField, BooleanType, ArrayType, IntegerType, MapType
from pyspark.sql import SparkSession
import json
from json import JSONDecodeError
from time import time

start = time()

def sparkSessionBuilder():
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
    return spark

def spark_dataframe(path):
    spark = sparkSessionBuilder()
    df = spark.read.format('csv').options(Header=True).option('inferSchema','True').load(path)
    df.repartition(5)
    return df

def recurse(d):
    try:
        if isinstance(d, dict):
            loaded_d = d
        else:
            loaded_d = json.loads(d)
        for k, v in loaded_d.items():
            loaded_d[k] = recurse(v)
    except (JSONDecodeError, TypeError):
        return d
    return loaded_d

    for d in data_list:
        for key, val in d.items():
            d[key] = recurse(val)

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





# dir_path = "/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/sample/School/"
dir_path = "/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/from_customer/innive_data/school/"

if __name__ == "__main__":
    abc = "schoolYearTypeReference_schoolYear"
    spark = sparkSessionBuilder()


    # base_columns = [str(c) for c in df_base.columns if c not in {"Operation","charterApprovalSchoolYearTypeReference_schoolYear"}]

    def transform_school_base(file_path = "School_Base.csv"):
        df_base = spark_dataframe(path=dir_path + file_path)
        df_base = df_base\
        .withColumn("charterApprovalSchoolYearTypeReference", to_json(struct(df_base.charterApprovalSchoolYearTypeReference_schoolYear.alias("schoolYear"))))\
        .withColumn("localEducationAgencyReference", to_json(struct(df_base.localEducationAgencyId)))
        return df_base

    def transform_school_addresses(file_path = "School_addresses.csv" ):
        df_addresses = spark_dataframe(path= dir_path + file_path)
        df_addresses = df_addresses.select("schoolId","Operation",
                                           array(to_json(struct([str(c) for c in df_addresses.columns if c not in {'schoolId','Operation'}]))).alias("addresses"))
        return df_addresses


    def transform_school_gradeLevels(filepath = "School_gradeLevels.csv"):
        df_gradeLevels = spark_dataframe(path= dir_path + filepath)
        df_gradeLevels = df_gradeLevels.select("schoolId","Operation", to_json(struct([str(c) for c in df_gradeLevels.columns if c not in {'schoolId','Operation'}])).alias("gradeLevels"))
        return df_gradeLevels

    def transform_internationalAddresses(filepath = None):
        df_intAdd = spark_dataframe(path= dir_path + filepath)
        df_intAdd = df_intAdd.select("schoolId","Operation",array(to_json(struct([str(c) for c in df_intAdd.columns if c not in {'schoolId','Operation'}])))
                                                                  .alias("internationalAddresses"))
        return df_intAdd

    def transform_identificationCodes(filepath = None):
        df_id = spark_dataframe(path= dir_path + filepath)
        df_id = df_id.select("schoolId", "Operation", array(to_json(struct([str(c)  for c in df_id.columns if c not in {'schoolId','Operation'}])))
                             .alias("identificationCodes"))
        return df_id


    def transform_indicators(filepath = None):
        df_indicators = spark_dataframe(path=dir_path + filepath)
        df_indicators = df_indicators.select("schoolId","Operation",
                                             array((to_json(struct([str(c)  for c in df_indicators.columns if c not in {'schoolId','Operation'}]))))
                                             .alias("indicators"))
        return df_indicators


    def transform_institutionTelephones(filepath = None):
        df_telephone = spark_dataframe(path=dir_path + filepath)
        df_telephone = df_telephone.select("schoolId","Operation",
                                           array(to_json(struct([str(c)  for c in df_telephone.columns if c not in {'schoolId','Operation'}])))
                                           .alias("institutionTelephones"))
        return df_telephone


    def transform_schoolCategories(filepath = None):
        df_schoolCat = spark_dataframe(path=dir_path + filepath)
        df_schoolCat = df_schoolCat.select("schoolId","Operation",
                                           array(to_json(struct([str(c)  for c in df_schoolCat.columns if c not in {'schoolId','Operation'}])))
                                           .alias("schoolCategories"))
        return df_schoolCat

    def transform_educationOrganizationCategories(filepath = None):
        df_eoc = spark_dataframe(path=dir_path + filepath)
        df_eoc = df_eoc.select("schoolId","Operation",
                                array(to_json(struct([str(c)  for c in df_eoc.columns if c not in {'schoolId','Operation'}])))
                               .alias("educationOrganizationCategories"))
        return df_eoc


    def unionAll(dfs, primary_key):
        from functools import reduce
        new_data = reduce(lambda df1, df2: df1.join(df2, primary_key, how='outer'), dfs)
        return new_data
    def get_joined_data():

        primary_key = ["schoolId","Operation"]

        df_base = transform_school_base(file_path = "School_Base.csv")
        df_gradeLevels = transform_school_addresses(file_path="School_addresses.csv")
        df_addresses = transform_school_gradeLevels(filepath="School_gradeLevels.csv")
        df_intAdd = transform_internationalAddresses(filepath="School_internationalAddresses.csv")
        df_idCode = transform_identificationCodes(filepath="School_identificationCodes.csv")
        df_indicators = transform_indicators(filepath="School_indicators.csv")
        df_tele = transform_institutionTelephones(filepath="School_institutionTelephones.csv")
        df_schCat = transform_schoolCategories(filepath="School_schoolCategories.csv")
        df_eoc = transform_educationOrganizationCategories(filepath="School_EducationOrganizationCategories.csv")

        dfs = [df_base,df_gradeLevels,df_addresses,df_intAdd,df_idCode, df_indicators, df_tele, df_schCat, df_eoc]
        df_school = unionAll(dfs=dfs, primary_key = ["schoolId","Operation"])
        df_school = df_school.select("schoolId", "Operation",
                                     to_json(
                                         struct(
                                             [str(c) for c in df_school.columns
                                              if c not in {'charterApprovalSchoolYearTypeReference_schoolYear','Operation'}])).alias("payload"))
        return df_school


    df_school = get_joined_data()
    print(df_school.count())
    df_school.show(truncate=False)

    end = time()
    total = end - start
    print(total)


    from pprint import pprint
    col_payload = df_school.select(df_school.payload).collect()
    list1 = [i[0] for i in col_payload][0]
    pprint(remove_empty_elements(recurse(list1)))


