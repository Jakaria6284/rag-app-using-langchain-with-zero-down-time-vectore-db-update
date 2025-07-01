from kafka import KafkaConsumer
import json
from llm_chain import get_answer
from config.settings import *

# Setup Kafka
consumer = KafkaConsumer(
    QUERY_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='query-consumer-group'
)

print("[Query Consumer] Ready to answer questions...")

for msg in consumer:
    try:
        query = msg.value['query']
        print(f"\n[Question] {query}")
        
        answer = get_answer(query)
        print(f"[Answer] {answer}")
        
    except Exception as e:
        print(f"[Error] {str(e)}")