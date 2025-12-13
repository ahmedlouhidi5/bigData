from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

while True:
    msg = input("Enter message: ")
    producer.send('test-topic', msg.encode('utf-8'))
    producer.flush()

