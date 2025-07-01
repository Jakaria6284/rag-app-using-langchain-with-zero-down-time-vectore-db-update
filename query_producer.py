from kafka import KafkaProducer
import json
from config.settings import *

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Customer Support Bot - Type 'exit' to quit")
while True:
    query = input("\nYour question: ")
    if query.lower() == 'exit':
        break
        
    producer.send(QUERY_TOPIC, {'query': query})
    producer.flush()
    print("[Sent] Processing your question...")

producer.close()