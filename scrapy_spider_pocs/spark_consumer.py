from kafka import KafkaConsumer
from json import loads


consumer = KafkaConsumer(
    'vix',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

for message in consumer:
    # print("1.",  message)
    message = message.value
    print("--->",message)