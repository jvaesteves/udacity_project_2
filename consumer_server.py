from kafka import KafkaConsumer
import json, time

consumer = KafkaConsumer('POLICE_CALLS',
    group_id='police_calls_consumer',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda message: json.loads(message.decode('utf-8'))
)

while True:
    consumer.poll(1000, max_records=10)

    for message in consumer:
        print(message.value)
    
    time.sleep(1.0)
