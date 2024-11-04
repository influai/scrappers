import logging
import time
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from telethon import TelegramClient, functions
from telethon.types import ChannelFull

from db_handler.database import Channel, Runs, Similars

from .msg_scrappers import scrape_msgs_batch


async def scrape_similar_channels(
    client: TelegramClient, channel_full: ChannelFull, db_session: Session
) -> None:
    """
    Scrapes the list of similar Telegram channels and adds them to the database.
    """
    result = await client(
        functions.channels.GetChannelRecommendationsRequest(channel=channel_full)
    )

    if result and result.chats:
        similars_data_batch = [
            {
                "base_channel_id": channel_full.id,
                "similar_channel_id": channel.id,
                "similar_channel_name": channel.username,
                "similar_channel_title": channel.title,
            }
            for channel in result.chats
        ]

        stmt = insert(Similars).values(similars_data_batch)
        stmt = stmt.on_conflict_do_update(
            index_elements=["base_channel_id", "similar_channel_id"],
            set_={
                "similar_channel_name": stmt.excluded.similar_channel_name,
                "similar_channel_title": stmt.excluded.similar_channel_title,
            },
        )
        db_session.execute(stmt)

        logging.info(
            f"Successfully upserted {len(similars_data_batch)} similar channels for channel ID {channel_full.id}"
        )


async def scrape_channel_metadata(
    client: TelegramClient, channel_url: str, db_session: Session
) -> int:
    """
    Scrape channel metadata and insert or update the channel in the database.
    """

    full = await client(functions.channels.GetFullChannelRequest(channel_url))
    full_channel = full.full_chat

    channel_data = {
        "id": full_channel.id,
        "name": full.chats[0].title,
        "url": channel_url,
        "participants": full_channel.participants_count,
        "last_pinned_msg_id": full_channel.pinned_msg_id,
        "about": full_channel.about,
    }

    stmt = insert(Channel).values(channel_data)
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={
            "name": stmt.excluded.name,
            "url": stmt.excluded.url,
            "participants": stmt.excluded.participants,
            "last_pinned_msg_id": stmt.excluded.last_pinned_msg_id,
            "about": stmt.excluded.about,
        },
    )
    db_session.execute(stmt)

    await scrape_similar_channels(client, full_channel, db_session)

    logging.info(f"Successfully scraped metadata for channel {channel_url}.")

    return full_channel.id


async def scrape_channel(
    client: TelegramClient,
    channel_url: str,
    from_date: datetime,
    to_date: datetime,
    db_session: Session,
) -> None:
    """
    Scrapes channel metadata and its posts between the specified date range.
    """
    launch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_time = time.time()

    # Check if channel already scraped (stupid check now, later need to change)
    if bool(db_session.query(Channel).filter_by(url=channel_url).first()):
        logging.info(f"Skipping {channel_url}, already processed")
        print(f"    {channel_url} - Skipped (already processed)")
        return

    # Scrape channel metadata (with similar channels)
    time.sleep(5)  # to avoid FloodWaitError
    channel_id = await scrape_channel_metadata(client, channel_url, db_session)


    print(f"  Processing posts for {channel_url}...")

    posts_scraped = 0
    batch_size = 250
    messages_batch = []

    logging.info(f"Starting to process posts from {from_date} to {to_date}")

    async for msg in client.iter_messages(
        channel_url, reverse=True, offset_date=from_date, limit=None, wait_time=3
    ):
        try:
            if msg.date <= to_date:
                messages_batch.append(msg)

                if len(messages_batch) >= batch_size:
                    await scrape_msgs_batch(
                        messages_batch, channel_id, channel_url, db_session
                    )
                    posts_scraped += len(messages_batch)
                    messages_batch = []
                    print(f"  {posts_scraped} posts processed so far...")
            else:
                break

        except Exception as e:
            logging.error(f"Error scraping message {msg.id} in {channel_url}: {e}")
            continue

    # Process any remaining messages
    if messages_batch:
        await scrape_msgs_batch(messages_batch, channel_id, channel_url, db_session)
        posts_scraped += len(messages_batch)

    # Save the run information (timestamps, post counts, etc.) to DB
    db_session.add(
        Runs(
            channel_id=channel_id,
            channel_url=channel_url,
            from_date=from_date,
            to_date=to_date,
            scrape_date=launch_time,
            posts_scraped=posts_scraped,
            exec_time=time.time() - start_time,
        )
    )

    logging.info(f"Finished scraping posts. Total posts scraped: {posts_scraped}")
