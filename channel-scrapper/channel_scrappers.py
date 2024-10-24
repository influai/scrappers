import json
import logging
import time
from datetime import datetime
from pathlib import Path

import msg_scrappers
import pandas as pd
import utils
from telethon import TelegramClient, functions
from telethon.types import ChannelFull
from tqdm.asyncio import tqdm


async def scrape_similar_channels(
    client: TelegramClient, channel_full: ChannelFull
) -> list | None:
    """
    Finds and returns a list of similar Telegram channels based on recommendations.
    """
    try:
        # Request similar channel recommendations via Telegram API
        result = await utils.safe_api_request(
            client(
                functions.channels.GetChannelRecommendationsRequest(
                    channel=channel_full
                )
            ),
            "retrieving similar channels",
        )

        # Return None if no recommendations are found
        if not result:
            return None

        # Process and collect similar channel details
        similar_channels = [
            {"username": ch.username, "title": ch.title, "id": ch.id}
            for ch in result.chats
        ]

        return similar_channels

    except Exception as e:
        logging.error(
            f"Error retrieving similar channels for channel ID {channel_full.id}: {e}"
        )
        return None


async def prepare_channel(client: TelegramClient, channel_url: str) -> Path | None:
    """
    Prepares a directory for the channel, saves channel metadata, and finds similar channels.
    Returns the path to the created channel directory.
    """
    try:
        # Retrieve full channel details via Telegram API
        full = await utils.safe_api_request(
            client(functions.channels.GetFullChannelRequest(channel_url)),
            "retrieving channel entity",
        )
        full_channel = full.full_chat

        # Extract channel metadata
        metadata = {
            "id": full_channel.id,
            "url": channel_url,
            "title": full.chats[0].title,
            "about": full_channel.about,
            "participants": full_channel.participants_count,
            "last_pinned_msg_id": full_channel.pinned_msg_id,
        }

        # Create a directory for the channel to store metadata and similar channels
        channel_dir = Path(f"data/{full_channel.id}")
        channel_dir.mkdir(exist_ok=True)

        # Save channel metadata to a JSON file
        metadata_path = channel_dir / "meta.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=4, ensure_ascii=False)

        logging.info(f"Successfully saved channel metadata to {metadata_path}")

        # Find and save similar channels
        similar_channels = await scrape_similar_channels(client, full_channel)
        similar_channels_path = channel_dir / "similar_channels.json"
        with open(similar_channels_path, "w") as f:
            json.dump(similar_channels, f, indent=4, ensure_ascii=False)

        return channel_dir

    except Exception as e:
        logging.error(f"Error retrieving channel entity for URL {channel_url}: {e}")
        return None


async def scrape_channel(
    client: TelegramClient,
    channel_url: str,
    from_date: datetime,
    to_date: datetime,
    runs_info: pd.DataFrame,
) -> tuple[list, pd.DataFrame]:
    """
    Scrapes messages from a Telegram channel between the specified date range,
    tracks the run info, and returns any errors and updated run info.
    """
    errors = []
    launch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_time = time.time()

    # Prepare channel directory and metadata
    channel_dir = await prepare_channel(client, channel_url)

    if not channel_dir:
        logging.error(f"Failed to prepare channel directory for {channel_url}")
        return errors, runs_info

    # Determine the most recent 'to_date' and earliest 'from_date' based on run history
    latest, oldest = utils.last_old_dates(runs_info, channel_url)
    if latest is not None and oldest is not None and from_date > oldest:
        from_date = latest

    posts_scraped = 0
    progress_bar = tqdm(desc="Processing posts", unit="post")

    logging.info(f"Starting to process posts from {from_date} to {to_date}")

    # Iterate through messages and scrape them
    async for msg in client.iter_messages(
        channel_url, reverse=True, offset_date=from_date, limit=None, wait_time=5
    ):
        try:
            if msg.date <= to_date:
                # Create a directory for each message within the channel's directory
                msg_dir = channel_dir / str(msg.id)
                msg_dir.mkdir(exist_ok=True)

                # Scrape and save the message data
                msg_scrappers.scrape_msg(msg, channel_url, msg_dir)

                posts_scraped += 1
                progress_bar.update(1)
            else:
                break

        except Exception as e:
            errors.append(e)
            logging.error(f"Error scraping message {msg.id} in {channel_url}: {e}")
            continue

    # Save the run information (timestamps, post counts, etc.)
    runs_info = utils.save_run(
        runs_info,
        channel_url,
        from_date,
        to_date,
        posts_scraped,
        launch_time,
        time.time() - start_time,
    )

    logging.info(f"Finished scraping posts. Total posts scraped: {posts_scraped}")

    return errors, runs_info
