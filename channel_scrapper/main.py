import logging
import os
from pathlib import Path
from time import sleep

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from tqdm import tqdm

from db_handler.connection import get_session

from .channel_scrappers import scrape_channel
from .utils import format_channel_url, load_configs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    filename=Path("channel_scrapper.log"),
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
logging.info("Channel scrapper has started.")

# Load channel scrapping configurations (date range)
from_date, to_date = load_configs(Path("config.yml"))
# Load @channelnames for CSV
csv_path = "parsed_channels_part2.csv"
df = pd.read_csv(csv_path)

# Load Telegram API creds from .env
load_dotenv()
REQUIRED_ENV_VARS = [
    "TG_API_ID",
    "TG_API_HASH",
    "TG_SESSION",
    "TG_DEVICE_MODEL",
    "TG_SYSTEM_VERSION",
    "TG_APP_VERSION",
]
missing_vars = [var for var in REQUIRED_ENV_VARS if os.getenv(var) is None]
if missing_vars:
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(missing_vars)}"
    )

# Initialize the Telegram client with loaded configurations
telegram_client = TelegramClient(
    session=os.getenv("TG_SESSION"),
    api_id=os.getenv("TG_API_ID"),
    api_hash=os.getenv("TG_API_HASH"),
    device_model=os.getenv("TG_DEVICE_MODEL"),
    system_version=os.getenv("TG_SYSTEM_VERSION"),
    app_version=os.getenv("TG_APP_VERSION"),
)


async def main() -> None:
    session_generator = get_session()

    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Scraping channels"):
        channel_url = format_channel_url(row["tg_name"])
        if not channel_url:
            logging.warning(f"Skipping invalid or missing tg_name for row {index}")
            continue

        db_session: Session = next(session_generator)
        logging.info(f"Starting scraping for channel: {channel_url}")

        while True:
            try:
                error = await scrape_channel(
                    telegram_client, channel_url, from_date, to_date, db_session
                )
                db_session.commit()

                if error:
                    logging.warning(
                        f"Error encountered while scraping {channel_url}: {error}"
                    )
                else:
                    logging.info(f"Scraping completed successfully for {channel_url}")
                sleep(3)
                break

            except FloodWaitError as fwe:
                wait_sec = fwe.seconds if fwe.seconds else 5
                msg = f"FloodWaitError({fwe.seconds}) encountered when scraping {channel_url}. Waiting for {wait_sec} seconds before retrying."
                logging.error(msg)
                print(msg)
                sleep(wait_sec)

            except Exception as ex:
                logging.error(
                    f"Skip scraping {channel_url} because of an error: {ex}",
                    exc_info=True,
                )
                break  # Exit the loop on non-FloodWaitError exceptions

        db_session.close()


with telegram_client:
    telegram_client.loop.run_until_complete(main())
