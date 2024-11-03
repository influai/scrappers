import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from telethon import TelegramClient
from tqdm import tqdm

from db_handler.connection import get_session

from .channel_scrappers import scrape_channel
from .utils import load_configs, format_channel_url


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
csv_path = "parsed_channels.csv"
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

        try:
            db_session: Session = next(session_generator)
            logging.info(f"Starting scraping for channel: {channel_url}")
            error = await scrape_channel(
                telegram_client, channel_url, from_date, to_date, db_session
            )

            if error:
                logging.warning(
                    f"Error encountered while scraping {channel_url}: {error}"
                )
            else:
                logging.info(f"Scraping completed successfully for {channel_url}")

        except Exception as ex:
            logging.error(f"An error occurred with {channel_url}: {ex}", exc_info=True)

        finally:
            db_session.close()


with telegram_client:
    telegram_client.loop.run_until_complete(main())
