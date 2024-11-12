import argparse
import logging
import os
from pathlib import Path
from time import sleep

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from telethon import TelegramClient
from telethon.errors import FloodWaitError

from db_handler.connection import get_session

from .channel_scrappers import scrape_channel
from .utils import format_channel_name, load_configs


# Load environment variables
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

# Set up argparse for command-line arguments
parser = argparse.ArgumentParser(description="Telegram channel scraper")
parser.add_argument(
    "--csv_path",
    required=True,
    help="Path to the CSV file containing channels to scrape",
)
args = parser.parse_args()

# Load channels to parse info from CSV specified in command line
channels_to_parse = pd.read_csv(args.csv_path)

# Set up logging directory based on TG_SESSION environment variable
session_name = os.getenv("TG_SESSION")
if not session_name:
    raise EnvironmentError("Environment variable TG_SESSION is not set.")

session_dir = Path("logs", session_name)
session_dir.mkdir(exist_ok=True, parents=True)
log_file = session_dir / "channel_scraper.log"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    filename=log_file,
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.getLogger("sqlalchemy.pool").setLevel(logging.INFO)
logging.info("Channel scraper has started.")

# Load channel scraping configurations (date range)
from_date, to_date = load_configs(Path("config.yml"))

# Initialize the Telegram client with loaded configurations
telegram_client = TelegramClient(
    session=Path(session_dir, session_name),
    api_id=os.getenv("TG_API_ID"),
    api_hash=os.getenv("TG_API_HASH"),
    device_model=os.getenv("TG_DEVICE_MODEL"),
    system_version=os.getenv("TG_SYSTEM_VERSION"),
    app_version=os.getenv("TG_APP_VERSION"),
)


async def main() -> None:
    session_generator = get_session()
    total_channels = channels_to_parse.shape[0]
    for index, row in channels_to_parse.iterrows():
        channel_name = format_channel_name(row["tg_name"])
        if not channel_name:
            logging.warning(f"Skipping invalid or missing tg_name for row {index}")
            print(
                f"{index+1}/{total_channels}: {channel_name} - Skipping due to invalid/missing tg_name"
            )
            continue

        try:
            print(
                f"{index+1}/{total_channels}: Starting scraping channel @{channel_name}"
            )

            db_session: Session = next(
                session_generator
            )  # new db session for every channel, as else i got disconnected for idle
            while True:
                try:
                    await scrape_channel(
                        telegram_client,
                        channel_name,
                        from_date,
                        to_date,
                        db_session,
                        scraper_name=session_name,
                    )
                    break
                except FloodWaitError as fwe:
                    logging.error(
                        f"{fwe}\nFloodWaitError({fwe.seconds}) encountered. Waiting for {fwe.seconds} seconds."
                    )
                    print(
                        f"{index+1}/{total_channels}: @{channel_name} - Waiting due to FloodWaitError ({fwe.seconds}s)"
                    )
                    sleep(fwe.seconds)

            logging.info(f"Scraping completed successfully for @{channel_name}")
            print(f"{index+1}/{total_channels}: @{channel_name} - Success")

        except Exception as ex:
            logging.error(f"Error scraping @{channel_name}: {ex}", exc_info=True)
            print(f"{index+1}/{total_channels}: @{channel_name} - Error: {ex}")


with telegram_client:
    telegram_client.loop.run_until_complete(main())
