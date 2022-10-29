
import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import types
from pyspark.sql.types import *


spark = SparkSession.builder.getOrCreate()

df_calendar_base = spark.read.format('csv').options(Header=True).load('/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/sample/calendar/Calendar_Base.csv')
df_calendar_base.createOrReplaceTempView('calendar_base')

df_calendar_gradeLevels = spark.read.format('csv').options(Header=True).load('/home/rohitg/PycharmProjects/spark_pocs/spark_based/innive_based/Data/sample/calendar/Calendar_gradeLevels.csv')

df_calendar_gradeLevels.createOrReplaceTempView('calendar_gradeLevels')


# sql_3 = "SELECT * FROM calendar_gradeLevels"

sql_1 = "SELECT " \
        "NULLIF(TRIM(operation),'') AS operation," \
        "NULLIF(TRIM(calendarCode),'') AS calendarCode," \
        "schoolId," \
        "schoolYear," \
        "NULLIF(TRIM(calendarTypeDescriptor),'') AS calendarTypeDescriptor" \
        " FROM calendar_base " \
        "WHERE " \
        "NULLIF(TRIM(calendarCode),'') IS NOT NULL " \
        "AND " \
        "schoolId IS NOT NULL AND " \
        "schoolYear IS NOT NULL " \
        "AND " \
        "NULLIF(TRIM(calendarTypeDescriptor),'') IS NOT NULL"

sql_2 = "SELECT " \
        "NULLIF(TRIM(Operation),'') AS Operation, " \
        "NULLIF(TRIM(calendarCode),'') AS calendarCode, " \
        "NULLIF(TRIM(gradeLevelDescriptor),'') AS gradeLevelDescriptor " \
        "FROM " \
        "calendar_gradeLevels " \
        "WHERE "  \
        "NULLIF(TRIM(calendarCode),'') IS NOT NULL AND " \
        "NULLIF(TRIM(gradeLevelDescriptor),'') IS NOT NULL" \
        ""

sql_df_calendar_base = spark.sql(sql_1)
sql_df_calendar_base.createOrReplaceTempView("cl_calendar_base")
# print('----1>',sql_df_calendar_base.show())

sql_df_calendar_gradeLevels = spark.sql(sql_2)
# print('--->',sql_df_calendar_gradeLevels.show())


sql_resource = "SELECT " \
               "cl_calendar_base.operation, " \
               "cl_calendar_base.calendarCode, " \
               "cl_calendar_base.schoolId, " \
               "cl_calendar_base.schoolYear, " \
               "NULLIF(TRIM(cl_calendar_base.calendarTypeDescriptor),'') AS calendarTypeDescriptor," \
               "to_json(map('schoolId', cl_calendar_base.schoolId)) AS SchoolRef," \
               "to_json(map('schoolYear',cl_calendar_base.schoolYear)) AS SYTRef " \
                " FROM " \
               "cl_calendar_base"


df_staging = spark.sql(sql_resource)
df_staging.createOrReplaceTempView('calendar_payload')


sample_payload = "SELECT calendar_payload.calendarCode FROM calendar_payload"


sql_resource_payload = "SELECT " \
                       "to_json(map(" \
                       "'calendarCode', calendar_payload.calendarCode," \
                       "'schoolReference',calendar_payload.schoolRef," \
                       "'schoolYearTypeReference',calendar_payload.SYTRef," \
                       "'calendarTypeDescriptor',calendar_payload.calendarTypeDescriptor," \
                       "'gradeLevels',calendar_gradeLevels.gradeLevelDescriptor" \
                       ")) AS PAYLOAD " \
                       "FROM " \
                       "calendar_payload " \
                       "LEFT JOIN calendar_gradeLevels " \
                       "ON calendar_payload.calendarCode = calendar_gradeLevels.calendarCode" \

import pandas as pd

df_resource = spark.sql(sql_resource_payload)
print(df_resource.show(truncate=False))


# print(df_resource.toPandas().to_csv("sample_payload_calendar.csv",index_label=False))

