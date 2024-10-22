import logging
from pathlib import Path

import channel_scrappers
import utils
from telethon import TelegramClient

# configure logging
logging.basicConfig(
    level=logging.INFO,
    filename=Path("channel_scrapper.log"),
    filemode="w",
    format="%(asctime)s %(levelname)s %(message)s",
)
logging.info("channel scrapper started")

# load configs
tg_client, channel_url, from_date, to_date = utils.load_configs(Path("config.yml"))

# load (or create and load) csv with runs info
runs_info_path = Path("data", "runs_info.csv")
run_info = utils.create_or_load_csv(runs_info_path)

# create tg client
tc = TelegramClient(**tg_client)


async def main():
    try:
        err, runs_info = await channel_scrappers.scrape_channel(
            tc, channel_url, from_date, to_date, run_info
        )
        if err:
            logging.warning(err)
        else:
            runs_info.to_csv(runs_info_path, index=False)
            logging.info(
                f"scrapping done successfully, saved updated runs info in {runs_info_path}"
            )

    except Exception as ex:
        logging.critical(ex)


with tc:
    tc.loop.run_until_complete(main())
