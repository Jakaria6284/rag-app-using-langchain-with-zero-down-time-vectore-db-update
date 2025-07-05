# main.py

from fastapi import FastAPI
from uuid import uuid4
from query_producer import send_query_to_kafka
from redis_client import redis_client
import asyncio

app = FastAPI()

@app.post("/ask/")
async def ask_question(question: str):
    query_id = str(uuid4())
    send_query_to_kafka(query_id, question)

    # Poll Redis for up to 10 seconds
    timeout = 10  # seconds
    interval = 0.5  # poll every 500ms
    elapsed = 0

    while elapsed < timeout:
        answer = redis_client.get(query_id)
        if answer:
            return {"query_id": query_id, "answer": answer.decode()}
        
        await asyncio.sleep(interval)
        elapsed += interval

    return {
        "message": "Answer not ready yet, please retry using /answer/{query_id}",
        "query_id": query_id
    }
