from kafka import KafkaProducer
import json, random, time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    sensor_value = random.randint(10, 100)
    message = {"sensor_id": 1, "value": sensor_value}

    producer.send("realtime-stream", message)
    print("Sent -> ", message)

    time.sleep(1)
