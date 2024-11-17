import asyncio
import json
import logging
import os
from typing import List

import aio_pika
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

class TaskManager:
    def __init__(self, queue_name: str = "tasks"):
        self.rabbit_params = {
            "host": os.getenv("RABBITMQ_HOST"),
            "port": int(os.getenv("RABBITMQ_PORT", "5672")),
            "login": os.getenv("RABBITMQ_LOGIN"),
            "password": os.getenv("RABBITMQ_PASSWORD"),
        }
        self.queue_name = queue_name

    async def connect(self) -> aio_pika.Connection:
        """Establish connection to RabbitMQ"""
        return await aio_pika.connect_robust(**self.rabbit_params)

    async def send_scrape_task(self, channel_name: str) -> None:
        """
        Send a single channel scraping task to the queue
        
        Args:
            channel_name: Channel username (with or without @ prefix)
        """
        # Ensure channel name starts with @
        if not channel_name.startswith('@'):
            channel_name = f"@{channel_name}"

        task = {
            "type": "scrape",
            "channel_name": channel_name
        }

        async with await self.connect() as connection:
            channel = await connection.channel()
            await channel.declare_queue(self.queue_name, durable=True)

            message = aio_pika.Message(
                body=json.dumps(task).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )

            await channel.default_exchange.publish(
                message,
                routing_key=self.queue_name
            )
            logging.info(f"Sent scraping task for channel: {channel_name}")

    async def send_bulk_scrape_tasks(self, channel_names: List[str]) -> None:
        """
        Send multiple channel scraping tasks to the queue
        
        Args:
            channel_names: List of channel usernames (with or without @ prefix)
        """
        async with await self.connect() as connection:
            channel = await connection.channel()
            await channel.declare_queue(self.queue_name, durable=True)

            for channel_name in channel_names:
                await self.send_scrape_task(channel_name)


async def main():
    manager = TaskManager()

    await manager.send_scrape_task("@example_channel")

    channels = ["@channel1", "channel2", "@channel3"]
    await manager.send_bulk_scrape_tasks(channels)

if __name__ == "__main__":
    asyncio.run(main())