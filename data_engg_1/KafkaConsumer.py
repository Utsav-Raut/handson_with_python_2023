from kafka import KafkaConsumer
import json

# Kafka broker details
bootstrap_servers = 'localhost:9092'
topic = 'input_topic'

# Create Kafka consumer
consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, 
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                         auto_offset_reset='earliest')

print("Hi")
# Subscribe to the Kafka topic
# consumer.subscribe([topic])
consumer.subscribe([topic])

# Consume and print data from the Kafka topic
try:
    for message in consumer:
        print(message)
        data = message.value
        print("Consumed data:", data)
except Exception as e:
    print("error occurred : ",e)
