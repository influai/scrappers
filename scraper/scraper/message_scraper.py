import logging
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Union

from database.database import Forwards, Post
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from telethon.tl.custom.message import Message
from telethon.tl.types import (
    GeoPoint,
    MessageEntityTextUrl,
    ReactionCustomEmoji,
    ReactionEmoji,
    ReactionPaid,
)


def scrape_geo(msg: Message) -> tuple[float, float] | None:
    """
    Extracts the longitude and latitude from a message's geo location (if available).

    Returns:
        tuple: Longitude and latitude as floats, or None if no geo data is present.
    """
    if isinstance(msg.geo, GeoPoint):
        return (msg.geo.long, msg.geo.lat)
    if msg.venue and isinstance(msg.venue.geo, GeoPoint):
        return (msg.venue.geo.long, msg.venue.geo.lat)
    return None


def scrape_poll(msg: Message) -> None | Dict[str, Union[str, List[str], int]]:
    """
    Extracts poll information from a message, including the question, answers, and total voters.

    Returns:
        dict: A dict containing the poll question, list of answers, and total voters, or None if no poll.
    """
    if not msg.poll:
        return None

    poll_info = {
        "question": msg.poll.poll.question.text,
        "answers": [answer.text.text for answer in msg.poll.poll.answers],
        "total_voters": msg.poll.results.total_voters,
    }
    return poll_info


def scrape_forward(msg: Message, to_ch_id: int) -> Forwards | None:
    """
    Extracts information about the original channel a message was forwarded from.
    """
    try:
        return Forwards(
            from_ch_id=msg.fwd_from.from_id.channel_id,
            from_post_id=msg.fwd_from.channel_post,
            to_ch_id=to_ch_id,
            to_post_id=msg.id,
        )
    except Exception:
        return None


def scrape_reactions(msg: Message) -> Tuple[int, dict, dict]:
    """
    Scrapes and saves reactions data from a message.
    """
    paid_reactions = 0
    stardard_reactions = {}
    custom_reactions = {}

    if msg.reactions:
        for reaction_cnt in msg.reactions.results:
            if isinstance(reaction_cnt.reaction, ReactionPaid):
                paid_reactions = reaction_cnt.count
            elif isinstance(reaction_cnt.reaction, ReactionEmoji):
                emoji = reaction_cnt.reaction.emoticon
                stardard_reactions[emoji] = reaction_cnt.count
            elif isinstance(reaction_cnt.reaction, ReactionCustomEmoji):
                emoji_id = reaction_cnt.reaction.document_id
                custom_reactions[emoji_id] = reaction_cnt.count

    return paid_reactions, stardard_reactions, custom_reactions


def scrape_urls(msg: Message) -> Dict:
    """
    Extracts URLs from message entities, if present.
    """
    urls = {}

    if msg.entities:
        for ent in msg.entities:
            if isinstance(ent, MessageEntityTextUrl):
                urls[ent.offset] = (ent.length, ent.url)
    return urls


def scrape_comments(msg: Message) -> int | None:
    """
    Scrapes the number of comments on a post, if comments are allowed.

    Args:
        msg (Message): The Telegram message object.

    Returns:
        int: The number of comments, or None if comments are not allowed.
    """
    if msg.replies:
        return msg.replies.replies
    return None


async def scrape_msgs_batch(
    messages: List[Message], channel_id: int, channel_name: str, db_session: Session
) -> None:
    post_data_batch = []
    forward_data_batch = []

    for msg in messages:
        paid_reactions, standard_reactions, custom_reactions = scrape_reactions(msg)

        post_data = {
            "channel_id": channel_id,
            "url": f"https://t.me/{channel_name}/{msg.id}",
            "post_date": msg.date,
            "scrape_date": datetime.now(tz=timezone.utc),
            "views": msg.views,
            "paid_reactions": paid_reactions,
            "forwards": msg.forwards,
            "comments": scrape_comments(msg),
            "silent": msg.silent,
            "is_post": msg.post,
            "noforwards": msg.noforwards,
            "pinned": msg.pinned,
            "via_bot_id": msg.via_bot_id,
            "via_business_bot_id": msg.via_business_bot_id,
            "fwd_from_flag": msg.fwd_from is not None,
            "photo": bool(msg.photo),
            "document": bool(msg.document),
            "web": bool(msg.web_preview),
            "audio": bool(msg.audio),
            "voice": bool(msg.voice),
            "video": bool(msg.video),
            "gif": bool(msg.gif),
            "geo": scrape_geo(msg),
            "poll": scrape_poll(msg),
            "standard_reactions": standard_reactions,
            "custom_reactions": custom_reactions,
            "raw_text": msg.raw_text,
            "format_text": msg.text,
            "urls": scrape_urls(msg),
        }
        post_data_batch.append(post_data)
        # Scrape forward data if the message is forwarded
        fwd_record = scrape_forward(msg, channel_id)
        if fwd_record:
            forward_data_batch.append(
                {
                    "from_ch_id": fwd_record.from_ch_id,
                    "from_post_id": fwd_record.from_post_id,
                    "to_ch_id": fwd_record.to_ch_id,
                    "to_post_id": fwd_record.to_post_id,
                }
            )

    # Bulk upsert posts
    if post_data_batch:
        post_stmt = insert(Post).values(post_data_batch)
        post_stmt = post_stmt.on_conflict_do_update(
            index_elements=["id"],
            set_={
                "channel_id": post_stmt.excluded.channel_id,
                "url": post_stmt.excluded.url,
                "post_date": post_stmt.excluded.post_date,
                "scrape_date": post_stmt.excluded.scrape_date,
                "views": post_stmt.excluded.views,
                "paid_reactions": post_stmt.excluded.paid_reactions,
                "forwards": post_stmt.excluded.forwards,
                "comments": post_stmt.excluded.comments,
                "silent": post_stmt.excluded.silent,
                "is_post": post_stmt.excluded.is_post,
                "noforwards": post_stmt.excluded.noforwards,
                "pinned": post_stmt.excluded.pinned,
                "via_bot_id": post_stmt.excluded.via_bot_id,
                "via_business_bot_id": post_stmt.excluded.via_business_bot_id,
                "fwd_from_flag": post_stmt.excluded.fwd_from_flag,
                "photo": post_stmt.excluded.photo,
                "document": post_stmt.excluded.document,
                "web": post_stmt.excluded.web,
                "audio": post_stmt.excluded.audio,
                "voice": post_stmt.excluded.voice,
                "video": post_stmt.excluded.video,
                "gif": post_stmt.excluded.gif,
                "geo": post_stmt.excluded.geo,
                "poll": post_stmt.excluded.poll,
                "standard_reactions": post_stmt.excluded.standard_reactions,
                "custom_reactions": post_stmt.excluded.custom_reactions,
                "raw_text": post_stmt.excluded.raw_text,
                "format_text": post_stmt.excluded.format_text,
                "urls": post_stmt.excluded.urls,
            },
        )
        db_session.execute(post_stmt)

    # Bulk insert forwards with conflict ignore (assuming only inserts)
    if forward_data_batch:
        forward_stmt = insert(Forwards).values(forward_data_batch)
        forward_stmt = forward_stmt.on_conflict_do_nothing()
        db_session.execute(forward_stmt)

    db_session.commit()
    logging.info(f"Successfully upserted {len(post_data_batch)} messages in batch")
