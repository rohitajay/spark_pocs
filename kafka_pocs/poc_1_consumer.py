from kafka import KafkaConsumer
# from pymongo import MongoClient
from json import loads

consumer = KafkaConsumer(
    'numtest',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))


# client = MongoClient('localhost:2701')
# collection = client.numtest.numtest

for message in consumer:
    # print("1.",  message)
    message = message.value
    print("--->",message)

    # collection.insert_one(message)

    # print('{} added to {}'.format(message, collection))