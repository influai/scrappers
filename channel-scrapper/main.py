import logging
from pathlib import Path

import channel_scrappers
import utils
from telethon import TelegramClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    filename=Path("channel_scrapper.log"),
    filemode="w",
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.info("Channel scrapper has started.")

# Load configurations (Telegram client credentials, channel URL, date range)
tg_client, channel_url, from_date, to_date = utils.load_configs(
    Path("configs", "config.yml")
)

# Load or create a CSV to track scraping runs
runs_info_path = Path("data", "runs_info.csv")
run_info = utils.create_or_load_csv(runs_info_path)

# Initialize the Telegram client with loaded configurations
telegram_client = TelegramClient(**tg_client)


async def main():
    try:
        # Scrape the channel based on provided parameters
        error, updated_run_info = await channel_scrappers.scrape_channel(
            telegram_client, channel_url, from_date, to_date, run_info
        )

        if error:
            logging.warning(f"Scraping encountered an error: {error}")
        else:
            # Save the updated run information to CSV and log success
            updated_run_info.to_csv(runs_info_path, index=False)
            logging.info(
                f"Scraping completed successfully. Updated run info saved to {runs_info_path}."
            )

    except Exception as ex:
        # Log critical errors with detailed information
        logging.critical(f"An unexpected error occurred: {ex}", exc_info=True)


# Run the asynchronous main function within the Telegram client's event loop
with telegram_client:
    telegram_client.loop.run_until_complete(main())
