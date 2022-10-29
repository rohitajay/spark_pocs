
import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import types
from pyspark.sql.types import *
import pandas as pd

spark = SparkSession.builder.getOrCreate()

df_calendar_base = spark.read.format('csv').options(Header=True).load('/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/sample/calendar/Calendar_Base.csv')

df_calendar_gradeLevels = spark.read.format('csv').options(Header=True).load('/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/sample/calendar/Calendar_gradeLevels.csv')

# Merge files
df_total = df_calendar_base.join(df_calendar_gradeLevels, ["Operation","calendarCode"])
# print(df_total.show(truncate=False))
df_total.createOrReplaceTempView("calendar_resource")

sql_resource_payload = "SELECT " \
                       "to_json(map('schoolId',calendar_resource.schoolId, 'calendarCode',calendar_resource.calendarCode)) AS PrimaryKey," \
                       "to_json(map(" \
                       "'calendarCode', calendar_resource.calendarCode," \
                       "'schoolReference',to_json(map('schoolId', calendar_resource.schoolId)) ," \
                       "'schoolYearTypeReference',to_json(map('schoolYear',calendar_resource.schoolYear)) ," \
                       "'calendarTypeDescriptor',calendar_resource.calendarTypeDescriptor," \
                       "'gradeLevels',calendar_resource.gradeLevelDescriptor" \
                       ")) AS PAYLOAD " \
                       "FROM " \
                       "calendar_resource " \
                       # "LEFT JOIN calendar_gradeLevels " \
                       # "ON calendar_payload.calendarCode = calendar_gradeLevels.calendarCode" \


df_cal_resource = spark.sql(sql_resource_payload)
# df_cal_resource.withColumn('primary_key', df_total.schoolId + ',' + df_total.calendarCode )
# print(df_cal_resource.show(truncate=False))


# df_pandas = df_total.toPandas()

# df_mod = df_pandas[['calendarCode','schoolId']].assign(payload = df_pandas.iloc[:,:].agg(pd.Series.to_json,1))
# print(df_mod.head())

# df_total.

# print(df_total.toJSON().collect())

# df_total.to


# dict_cal = map(lambda row: row.asDict(), df_calendar_base.collect())
# print(dict_cal)

# df_calendar_base.createOrReplaceTempView('calendar_base')


# df_calendar_gradeLevels.createOrReplaceTempView('calendar_gradeLevels')

