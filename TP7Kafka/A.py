from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

machines = ['Machine_1', 'Machine_2', 'Machine_3']

while True:
    data = {
        'machine': random.choice(machines),
        'temperature': round(random.uniform(20, 100), 2),  # درجة حرارة عشوائية
        'status': random.choice(['OK', 'FAIL']),           # حالة الماكينة
        'timestamp': int(time.time())
    }
    producer.send('test-topic', value=data)
    print("Sent:", data)
    time.sleep(2)  # كل ثانيتين رسالة جديدة
