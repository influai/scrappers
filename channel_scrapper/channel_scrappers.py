import logging
import time
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from telethon import TelegramClient, functions
from telethon.types import InputPeerChannel

from db_handler.database import Channel, Runs, Similars, Peers

from .msg_scrappers import scrape_msgs_batch


SLEEP_BETWEEN_CHANNELS = 15


async def get_channel_peer(
        channel_name: str, 
        scraper_name: str,
        db_session: Session,
        client: TelegramClient
) -> InputPeerChannel:
    """
    Get the channel peer to corresponding scraper.

    The function works as follows:
    1) Checks if peer data exists in the database and retrieves it.
    2) If it doesn't exist, calls `get_input_entity` (which may have a high timeout) to fetch the data.
    
    Note: The access_hash is unique to each account (scraper).
    """
    # Query the database for an existing peer
    result = db_session.execute(
        select(Peers).where(
            Peers.channel_name == channel_name,
            Peers.scraper_name == scraper_name
        )
    )
    peer = result.scalar_one_or_none()

    if peer:
        msg = f"Successfully loaded peer for channel @{channel_name}"
        logging.info(msg)
        print(msg)
        return InputPeerChannel(channel_id=peer.id, access_hash=peer.access_hash)
    
    msg = f"Peer not found for @{channel_name}\nCalling ResolveUsername and sleep for {SLEEP_BETWEEN_CHANNELS}s after to avoid FloodWaitError"
    logging.info(msg)
    print(msg)

    # Fetch the peer data from Telegram API
    entity = await client.get_input_entity(channel_name)
    assert isinstance(entity, InputPeerChannel)
    # Save new peer data to the database
    new_peer = Peers(
        id=entity.channel_id,
        channel_name=channel_name,
        scraper_name=scraper_name,
        channel_id=entity.channel_id,
        access_hash=entity.access_hash
    )
    db_session.add(new_peer)
    db_session.commit()

    time.sleep(SLEEP_BETWEEN_CHANNELS)  # to avoid FloodWaitError for ResolveUsernameRequest
    
    return entity


async def scrape_similar_channels(
    client: TelegramClient,
    channel: InputPeerChannel,
    channel_name: str,
    db_session: Session,
) -> None:
    """
    Scrapes the list of similar Telegram channels and adds them to the database.
    """
    result = await client(functions.channels.GetChannelRecommendationsRequest(channel))

    if result and result.chats:
        similars_data_batch = [
            {
                "base_channel_id": channel.channel_id,
                "similar_channel_id": sim_channel.id,
                "similar_channel_name": sim_channel.username,
                "similar_channel_title": sim_channel.title,
            }
            for sim_channel in result.chats
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
            f"Successfully upserted {len(similars_data_batch)} similar channels for channel @{channel_name}"
        )


async def scrape_channel_metadata(
    client: TelegramClient,
    channel: InputPeerChannel,
    channel_name: str,
    db_session: Session,
):
    """
    Scrape channel metadata and insert or update the channel in the database.
    """

    full = await client(functions.channels.GetFullChannelRequest(channel))
    full_channel = full.full_chat

    channel_data = {
        "id": full_channel.id,
        "name": full.chats[0].title,
        "url": "https://t.me/" + channel_name,
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

    await scrape_similar_channels(client, channel, channel_name, db_session)

    logging.info(f"Successfully scraped metadata for channel @{channel_name}.")


async def scrape_channel(
    client: TelegramClient,
    channel_name: str,
    from_date: datetime,
    to_date: datetime,
    db_session: Session,
    scraper_name: str,
) -> None:
    """
    Scrapes channel metadata and its posts between the specified date range.
    """
    launch_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    start_time = time.time()

    # Check if channel already scraped (stupid check now, later need to change)
    if bool(
        db_session.query(Channel).filter_by(url="https://t.me/" + channel_name).first()
    ):
        logging.info(f"Skipping @{channel_name}, already processed")
        print(f"    @{channel_name} - Skipped (already processed)")
        return
    
    # Get channel peer
    channel: InputPeerChannel = await get_channel_peer(channel_name, scraper_name, db_session, client)

    # Scrape channel metadata (with similar channels)
    await scrape_channel_metadata(client, channel, channel_name, db_session)

    print(f"  Processing posts for @{channel_name}...")

    posts_scraped = 0
    batch_size = 250
    messages_batch = []

    logging.info(f"Starting to process posts from {from_date} to {to_date}")

    async for msg in client.iter_messages(
        channel, reverse=True, offset_date=from_date, limit=None, wait_time=3
    ):
        if msg.date <= to_date:
            messages_batch.append(msg)

            if len(messages_batch) >= batch_size:
                await scrape_msgs_batch(
                    messages_batch, channel.channel_id, channel_name, db_session
                )
                posts_scraped += len(messages_batch)
                messages_batch = []
                print(f"  {posts_scraped} posts processed so far...")
        else:
            break

    # Process any remaining messages
    if messages_batch:
        await scrape_msgs_batch(
            messages_batch, channel.channel_id, channel_name, db_session
        )
        posts_scraped += len(messages_batch)

    # Save the run information (timestamps, post counts, etc.) to DB
    db_session.add(
        Runs(
            channel_id=channel.channel_id,
            channel_url="https://t.me/" + channel_name,
            from_date=from_date,
            to_date=to_date,
            scrape_date=launch_time,
            posts_scraped=posts_scraped,
            exec_time=time.time() - start_time,
        )
    )

    logging.info(f"Finished scraping posts. Total posts scraped: {posts_scraped}")
