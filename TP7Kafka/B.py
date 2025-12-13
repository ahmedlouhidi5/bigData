from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='iot-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening for IoT messages...")
for msg in consumer:
    data = msg.value
    print(f"Machine: {data['machine']}, Temp: {data['temperature']}°C, Status: {data['status']}, Time: {data['timestamp']}")
