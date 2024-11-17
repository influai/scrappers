import json
import os

import pika
from contracts import ChannelList
from fastapi import FastAPI, HTTPException


app = FastAPI()

# Get the env vars setted in docker-compose
parameters = pika.ConnectionParameters(
    host=os.getenv("RABBITMQ_HOST"),
    port=int(os.getenv("RABBITMQ_PORT")),
    credentials=pika.PlainCredentials(
        username=os.getenv("RABBITMQ_USER"),
        password=os.getenv("RABBITMQ_PASS"),
    ),
)
task_q: str = "tasks"


def send_task_to_queue(task: dict) -> None:
    """Sends a task to the RabbitMQ queue."""
    connection = pika.BlockingConnection(parameters=parameters)
    channel = connection.channel()
    channel.queue_declare(queue=task_q, durable=True)
    channel.basic_publish(
        exchange="",
        routing_key=task_q,
        body=json.dumps(task).encode(),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    connection.close()


@app.post("/scrape_channels/")
async def scrape_channels(channel_list: ChannelList) -> dict[str, str]:
    """Accepts a JSON file with channel names and queues scraping tasks."""
    if not channel_list.channels:
        raise HTTPException(status_code=400, detail="Channel list cannot be empty")

    for channel_name in channel_list.channels:
        if len(channel_name) > 1 and channel_name[1:].isalnum():
            task = {
                "type": "scrape",
                "channel_name": channel_name,
            }
            send_task_to_queue(task)

    return {"message": "Channels queued for scraping."}
