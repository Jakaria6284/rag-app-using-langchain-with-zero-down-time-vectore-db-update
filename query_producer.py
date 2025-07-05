# kafka_producer.py

from kafka import KafkaProducer
import json
from config.settings import KAFKA_BOOTSTRAP_SERVERS, QUERY_TOPIC

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_query_to_kafka(query_id: str, query: str):
    data = {"query_id": query_id, "query": query}
    producer.send(QUERY_TOPIC, data)
    producer.flush()
