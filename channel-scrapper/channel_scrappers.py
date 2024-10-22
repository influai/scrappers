import json
import logging
import time
from datetime import datetime
from pathlib import Path

import msg_scrappers
import pandas as pd
import utils
from telethon import TelegramClient, functions
from tqdm.asyncio import tqdm


async def prepare_channel(client: TelegramClient, channel_url: str) -> Path:
    """create channel dir and extract and save channel metadata"""
    # extract metadata
    #  https://tl.telethon.dev/methods/channels/get_messages.html
    chatfull = await client(functions.channels.GetFullChannelRequest(channel_url))
    metadata = {
        "id": chatfull.full_chat.id,
        "url": channel_url,
        "title": chatfull.chats[0].title,
        "about": chatfull.full_chat.about,
        "participants": chatfull.full_chat.participants_count,
        "last_pinned_msg_id": chatfull.full_chat.pinned_msg_id,
    }

    # create channel dir
    channel_dir = Path("data/" + str(chatfull.full_chat.id))
    channel_dir.mkdir(exist_ok=True)

    # save metadata
    with open(channel_dir / "meta.json", "w") as f:
        json.dump(metadata, f, indent=4, ensure_ascii=False)

    logging.info(
        f'successfully get channel metadata and save in {channel_dir / "meta.json"}'
    )
    return channel_dir


async def scrape_channel(
    client: TelegramClient,
    channel_url: str,
    from_date: datetime,
    to_date: datetime,
    runs_info: pd.DataFrame,
) -> tuple[list, pd.DataFrame]:
    err = []

    launch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_time = time.time()

    # extract channel metadata + create dir
    channel_dir = await prepare_channel(client, channel_url)

    # find lastest 'to_date' and oldest 'from_date' from 'runs_info'
    lastest, oldest = utils.last_old_dates(runs_info, channel_url)
    # change the from_date based on this
    if lastest is not None and oldest is not None:
        if from_date > oldest:
            from_date = lastest

    posts_scrapped = 0
    pbar = tqdm(desc="processing posts", unit="post")

    logging.info(f"starting the posts processing from {from_date} to {to_date}")
    # wait_time is needed because we dont want to get banned :)
    async for msg in client.iter_messages(
        channel_url, reverse=True, offset_date=from_date, limit=None, wait_time=10
    ):
        try:
            if msg.date <= to_date:
                # create msg dir for each msg inside channel dir
                msg_dir = channel_dir / str(msg.id)
                msg_dir.mkdir(exist_ok=True)

                msg_scrappers.scrape_msg(msg, channel_url, msg_dir)

                posts_scrapped += 1
                pbar.update(1)
            else:
                break

        except Exception as e:
            err.append(e)
            continue

    # save run info
    runs_info = utils.save_run(
        runs_info,
        channel_url,
        from_date,
        to_date,
        posts_scrapped,
        launch_time,
        time.time() - start_time,
    )
    return err, runs_info
