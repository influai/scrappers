import asyncio
import json
import logging
import os
from datetime import datetime

import aio_pika
from aio_pika.abc import AbstractIncomingMessage, AbstractRobustConnection
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession

from scraper.scrapers import ChannelScraper

# Load environment variables
load_dotenv()  # load creds (TG, RabbitMQ) from .env frovided via --env-file in docker run

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


class Consumer:
    """
    Represents task consumer from RabbitMQ queue and then starts the channel scraper
    """

    def __init__(self) -> None:
        self.tg_creds: dict = {
            "session": StringSession(os.getenv("TG_SESSION")),
            "api_id": int(os.getenv("TG_API_ID")),
            "api_hash": os.getenv("TG_API_HASH"),
            "device_model": os.getenv("TG_DEVICE_MODEL"),
            "system_version": os.getenv("TG_SYSTEM_VERSION"),
            "app_version": os.getenv("TG_APP_VERSION"),
        }
        self.scraper_id: int = self.tg_creds["api_id"]

        self.rabbit_creds: dict = {
            "host": os.getenv("RABBITMQ_HOST"),
            "port": int(os.getenv("RABBITMQ_PORT")),
            "login": os.getenv("RABBITMQ_LOGIN"),
            "password": os.getenv("RABBITMQ_PASSWORD"),
        }
        self.task_queue: str = "tasks"

    async def create_tg_scraper(self) -> ChannelScraper:
        """
        Create TelegramClient, connects to the Telegram and return ChannelScraper object
        """
        tg_client = TelegramClient(**self.tg_creds)
        await tg_client.connect()

        return ChannelScraper(tg_client, self.scraper_id)

    async def rabbit_connect(self) -> AbstractRobustConnection:
        """
        Establishes a RabbitMQ connection
        """
        return await aio_pika.connect_robust(**self.rabbit_creds)

    async def process_task(self, msg: AbstractIncomingMessage) -> None:
        """
        Process task, calling the scraping method.
        If processing fails, requeue the task to the end of the queue.
        """
        try:
            task = json.loads(msg.body.decode())

            if task.get("type") == "scrape":
                ack_flag: bool = await self.scraper.scrape(
                    task.get("channel_name"), datetime.strptime(task.get("from_date"), "%d-%m-%Y")
                )
                if ack_flag:
                    await msg.ack()
                else:
                    logging.warning(f"Task failed. Requeuing task: {task}")
                    await self.requeue_to_end(task)
                    await msg.ack()  # Remove the message from the current processing
            else:
                logging.warning(f"Unknown task type: {task.get('type')}")
                await msg.ack()
        except Exception as e:
            # If an error occurs during task processing, remove the message
            logging.warning(f"Error while decoding/deserializing message\n{e}")
            await msg.ack()
            raise

    async def requeue_to_end(self, task: dict) -> None:
        """
        Requeue the task to the end of the queue.
        """
        try:
            async with await self.rabbit_connect() as rabbit_conn:
                rabbit_channel = await rabbit_conn.channel()

                await rabbit_channel.default_exchange.publish(
                    aio_pika.Message(
                        body=json.dumps(task).encode(),
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    ),
                    routing_key=self.task_queue,
                )
                logging.info("Task requeued successfully.")
        except Exception as e:
            logging.error(
                "Failed to requeue message to end of the queue. Error details:", exc_info=True
            )

    async def run(self):
        """
        Launch the consuming action from the queue
        """
        self.scraper = await self.create_tg_scraper()

        async with await self.rabbit_connect() as rabbit_conn:
            rabbit_channel = await rabbit_conn.channel()
            await rabbit_channel.set_qos(prefetch_count=1)

            queue = await rabbit_channel.declare_queue(self.task_queue, durable=True)
            await queue.consume(self.process_task)

            logging.info(f"Successfully subscribed to '{self.task_queue}'")

            await asyncio.Future()


async def main():
    logging.info("Consumer started")

    consumer = Consumer()
    await consumer.run()


if __name__ == "__main__":
    asyncio.run(main())
