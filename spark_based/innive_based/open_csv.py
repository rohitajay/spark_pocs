import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark = SparkSession.builder.master("local[*]").appName("innive_data").getOrCreate()

# df = spark.read.csv("/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/sample/SSA/SSA_Base.csv")
# df.printSchema()


df_base = spark.read.csv("/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/sample/SSA/SSA_Base.csv", header=True)
df_agp = spark.read.csv("/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/sample/SSA/SSA_alternateGraduationPlanReference.csv",header=True)

df_education = spark.read.csv("/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/sample/SSA/SSA_educationPlans.csv",header=True)
#
# df_base.createOrReplaceTempView("base")
# df_agp.createOrReplaceTempView("agp")
# df_education.createOrReplaceTempView("education")



temp_table = spark.sql(
    "SELECT * "
    "FROM education"
)
#
# temp_table = spark.sql("json_build_object(SELECT  "
#                        "*"
#                        # "agp.alternativeGraduationPlanReference_educationOrganizationId"
#                        # " educationPlans_educationPlanDescriptor"
#                        " FROM base "
#                        "LEFT OUTER JOIN agp "
#                        "ON agp.studentUniqueId = base.studentUniqueId "
#                        "LEFT OUTER JOIN education"
#                        " ON education.studentUniqueId = agp.studentUniqueId);"
#                        # " agp.alternativeGraduationPlanReference_graduationPlanTypeDescriptor,"
#                        # "education.educationPlans_educationPlanDescriptor"
#                        # " "
#                        # "INNER JOIN agp ON base.studentUniqueId = agp.studentUniqueId AND "
#                        # "AND base.schoolId = agp.schoolId"
#                        # "INNER JOIN education ON"
#                        # "agp.studentUniqueId = education.studentUniqueId "
#                        # "AND agp.schoolId = education.schoolId"
#                        )
from pprint import pprint
pprint(temp_table.collect())




# print(df_base.show(5))