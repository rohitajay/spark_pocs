from time import sleep
from json import dumps
from kafka import KafkaProducer

from kafka import KafkaClient

print(dir(kafka.producer))
"""Reference: https://towardsdatascience.com/kafka-python-explained-in-10-lines-of-code-800e3e07dad1"""


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

for e in range(1000):
    data = {'number' : e*2}
    producer.send('numtest', value=data)
    sleep(1)