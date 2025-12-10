from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='kafka:29092',   # not kafka:9092
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


while True:
    message = {
        'sensor_id': random.randint(1, 5),
        'temperature': random.randint(20, 35),
        'humidity': random.randint(30, 70)
    }
    producer.send('test-topic', value=message)
    print("Message sent:", message)
    time.sleep(2)
