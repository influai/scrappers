import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import aio_pika
from database.connection import get_session
from sqlalchemy.orm import Session
from telethon import TelegramClient
from telethon.errors import FloodWaitError, UsernameNotOccupiedError
from telethon.sessions import StringSession

from scraper.channel_scraper import scrape_channel

# Load environmental variables, setted in docker-compose
rabbit_params: dict = {
    "host": os.getenv("RABBITMQ_HOST"),
    "port": int(os.getenv("RABBITMQ_PORT")),
    "login": os.getenv("RABBITMQ_USER"),
    "password": os.getenv("RABBITMQ_PASS"),
}
tg_params: dict = {
    "session": StringSession(os.getenv("TG_SESSION")),
    "api_id": int(os.getenv("TG_API_ID")),
    "api_hash": os.getenv("TG_API_HASH"),
    "device_model": os.getenv("TG_DEVICE_MODEL"),
    "system_version": os.getenv("TG_SYSTEM_VERSION"),
    "app_version": os.getenv("TG_APP_VERSION"),
}

scraper_id: int = tg_params["api_id"]

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

# RabbitMQ queue
task_q: str = "tasks"


async def process_task(client: TelegramClient, task: dict) -> bool:
    """
    Processes a single task received from the queue.

    Return the boolean `ack_flag`.
    When return `ack_flag` as True, that implies that the task was completed successfully 
    and it should be removed from the queue.
    When return `ack_flag` as False, that implies that the task couldn't be completed 
    and should be returned in queue, when in future another worker tries to do this task again.

    TODO: Expand the types of errors that can occur during channel scraping
    and determine whether a task should be removed from the queue at a particular error or not. 
    Now I consider only FloodWaitError and UsernameNotOccupiedError, in fact there are many more of them.
    """
    task_type: str = task.get("type")
    if task_type == "scrape":
        channel_name = task.get("channel_name")[
            1:
        ]  # get and conver from '@channel' to 'channel'
        logging.info(f"Processing parse task for channel: {channel_name}")
        from_date = datetime.strptime("17-11-2023", "%d-%m-%Y").replace(
            tzinfo=timezone.utc
        )
        db_session_generator = get_session()
        db_session: Session = next(db_session_generator)
        try:
            await scrape_channel(
                client,
                channel_name,
                from_date,
                db_session,
                scraper_id,
            )
            logging.info(f"Successfully scraped channel {channel_name}")
            return True

        except FloodWaitError as fwe:
            logging.error(f"Error processing task: {fwe}")
            return False
        except UsernameNotOccupiedError as unoe:
            logging.error(f"Error processing task: {unoe}")
            return True
        except Exception as e:
            logging.error(f"Error processing task: {e}", exc_info=True)
            return False
    else:
        logging.warning(f"Unknown task type: {task_type}")
        return True


async def main():
    logging.info("Scraper started")

    tg_cli = TelegramClient(**tg_params)
    await tg_cli.connect()

    # Connect to RabbitMQ
    connection = await aio_pika.connect_robust(**rabbit_params)

    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        queue = await channel.declare_queue(task_q, durable=True)

        async def process_message(message: aio_pika.IncomingMessage):
            async with message.process(ignore_processed=True):
                task = json.loads(message.body.decode())
                is_ack: bool = await process_task(tg_cli, task)
                if is_ack:
                    await message.ack()
                else:
                    await message.reject()

        await queue.consume(process_message)

        try:
            await asyncio.Future()
        finally:
            await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
