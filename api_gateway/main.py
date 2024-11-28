import json
import os
from datetime import datetime
import pika
from dotenv import load_dotenv
from contracts import ScrapingTasksList
from fastapi import FastAPI, HTTPException


app = FastAPI()

# Get the env vars
load_dotenv()  # Load RabbitMQ creds (can load .env file via Docker Volume)
parameters = pika.ConnectionParameters(
    host=os.getenv("RABBITMQ_HOST"),
    port=int(os.getenv("RABBITMQ_PORT")),
    credentials=pika.PlainCredentials(
        username=os.getenv("RABBITMQ_LOGIN"),
        password=os.getenv("RABBITMQ_PASSWORD"),
    ),
)
task_q: str = "tasks"


def send_task_to_queue(task: dict) -> None:
    """
    Sends a task to the RabbitMQ queue.
    """
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
async def scrape_channels(task_list: ScrapingTasksList) -> dict[str, str]:
    """
    Accepts a JSON file with channel names and scraping cutoff date and queues scraping tasks.
    """
    if not task_list.channels:
        raise HTTPException(status_code=400, detail="Channel list cannot be empty")

    # Validate `from_date` format
    try:
        datetime.strptime(task_list.from_date, "%d-%m-%Y")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format. Use 'DD-MM-YYYY'."
        )

    for channel_name in task_list.channels:
        task = {
            "type": "scrape",
            "channel_name": channel_name,
            "from_date": task_list.from_date,
        }
        send_task_to_queue(task)

    return {"message": "Channels queued for scraping."}
