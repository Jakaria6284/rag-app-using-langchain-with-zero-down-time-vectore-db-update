# kafka_consumer.py

from kafka import KafkaConsumer
import json
from config.settings import KAFKA_BOOTSTRAP_SERVERS, QUERY_TOPIC
from llm_chain import get_answer
from redis_client import redis_client

def run_consumer():
    consumer = KafkaConsumer(
        QUERY_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='query-consumer-group'
    )

    print("[Consumer] Listening for incoming queries...")

    for msg in consumer:
        try:
            data = msg.value
            query_id = data['query_id']
            query = data['query']
            print(f"[Received] {query_id}: {query}")

            answer = get_answer(query)
            redis_client.set(query_id, answer)

            print(f"[Stored] Answer for {query_id}")
        except Exception as e:
            print(f"[Error] {e}")

# To run directly:
if __name__ == '__main__':
    run_consumer()
