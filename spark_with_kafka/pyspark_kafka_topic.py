from time import time
from json import dumps
from kafka import KafkaProducer
import pandas as pd
import json
import numpy as np

data = pd.read_parquet("/home/rohitg/PycharmProjects/nyc_taxi/data/october/green_tripdata_2022-10.parquet")
data["lpep_pickup_datetime"] = data["lpep_pickup_datetime"].astype(str)
data["lpep_dropoff_datetime"] = data["lpep_dropoff_datetime"].astype(str)
max_id = data.shape[0]
data["id"] = np.arange(0, max_id)
# dict_data = data.to_dict('records')



def kafka_produce_stream(df,):

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:
                             dumps(x).encode('utf-8'))
    dict_data = df.to_dict("records")
    for e in range(1000):
        # print(dict_data[e])
        producer.send("green_october_1", value=dict_data[e],key=json.dumps(dict_data[e]["id"]).encode('utf-8'))
    print("process completed")

    return 1

run_producer = kafka_produce_stream(df=data)

from pyspark.sql.functions import *
from pyspark.sql.types import *

#import library
import os
from pyspark.sql import SparkSession



def read_from_kafka():
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

    sc = SparkSession.builder.appName('Pyspark_kafka_airline_read_write')\
        .getOrCreate()

    df = sc \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "green_october_1") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load() \
        .select("value") \
        .selectExpr("CAST(value AS STRING) as json")

    jsonSchema = StructType([StructField("id", StringType(), True),
                             StructField("VendorID", StringType(), True),
                                StructField("lpep_pickup_datetime", StringType(), True),
                                StructField("lpep_dropoff_datetime", StringType(), True),
                                StructField("store_and_fwd_flag", StringType(), True),
                                StructField("RatecodeID", StringType(), True),
                                StructField("PULocationID", StringType(), True),
                                StructField("DOLocationID", StringType(), True),
                                StructField("passenger_count", StringType(), True),
                                StructField("trip_distance", StringType(), True),
                                StructField("fare_amount", StringType(), True),
                                StructField("trip_distance", StringType(), True),
                                StructField("extra", StringType(), True),
                                StructField("mta_tax", StringType(), True),
                                StructField("tip_amount", StringType(), True),
                                StructField("tolls_amount", StringType(), True),
                                StructField("ehail_fee", StringType(), True),
                                StructField("improvement_surcharge", StringType(), True),
                                StructField("total_amount", StringType(), True),
                                StructField("payment_type", StringType(), True),
                                StructField("trip_type", StringType(), True),
                                StructField("congestion_surcharge", StringType(), True),])


    df = df.withColumn("jsonData", from_json(col("json"), jsonSchema)) \
                    .select("jsonData.*")
    return df

# df.show(100, truncate=False)
df = read_from_kafka()

print(df.show(10,truncate=False))
