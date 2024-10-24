import json
import logging
from pathlib import Path

from telethon.tl.custom.message import Message
from telethon.tl.types import (
    GeoPoint,
    MessageEntityTextUrl,
    MessageFwdHeader,
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


def scrape_poll(msg: Message):
    """
    Extracts poll information from a message, including the question, answers, and total voters.

    Returns:
        list: A list containing the poll question, list of answers, and total voters, or None if no poll.
    """
    if not msg.poll:
        return None

    poll_info = [
        msg.poll.poll.question.text,
        [answer.text.text for answer in msg.poll.poll.answers],
        msg.poll.results.total_voters,
    ]
    return poll_info


def scrape_forward_from_info(fwd_from: MessageFwdHeader) -> dict:
    """
    Extracts information about the original channel a message was forwarded from.

    Returns:
        dict: Contains the channel ID, channel name, and the original channel post ID.
    """
    return {
        "channel_id": fwd_from.from_id.channel_id,
        "name": fwd_from.from_name,
        "channel_post": fwd_from.channel_post,
    }


def scrape_reactions(msg: Message, msg_dir: Path) -> int:
    """
    Scrapes and saves reactions data from a message.

    Args:
        msg (Message): The Telegram message object containing reaction data.
        msg_dir (Path): Directory path where reaction data will be saved.

    Returns:
        int: The number of paid reactions (if any).
    """
    paid_reactions = 0
    reactions_data = {"reaction_emoji": {}, "custom_emoji": {}}

    if msg.reactions:
        for reaction_cnt in msg.reactions.results:
            if isinstance(reaction_cnt.reaction, ReactionPaid):
                paid_reactions = reaction_cnt.count
            elif isinstance(reaction_cnt.reaction, ReactionEmoji):
                emoji = reaction_cnt.reaction.emoticon
                reactions_data["reaction_emoji"][emoji] = reaction_cnt.count
            elif isinstance(reaction_cnt.reaction, ReactionCustomEmoji):
                emoji_id = reaction_cnt.reaction.document_id
                reactions_data["custom_emoji"][emoji_id] = reaction_cnt.count

    # Save reaction data to a JSON file
    with open(msg_dir / "reactions.json", "w") as f:
        json.dump(reactions_data, f, indent=4, ensure_ascii=False)

    return paid_reactions


def scrape_metadata(
    msg: Message,
    msg_dir: Path,
    channel_url: str,
    paid_reactions: int,
) -> None:
    """
    Scrapes message metadata and saves it along with paid reaction counts.

    Args:
        msg (Message): The Telegram message object.
        msg_dir (Path): Directory path where metadata will be saved.
        channel_url (str): The URL of the Telegram channel.
        paid_reactions (int): The count of paid reactions to the message.
    """
    msg_metadata = {
        "id": msg.id,
        "url": f"{channel_url}/{msg.id}",
        "date": str(msg.date),
        "views": msg.views,
        "paid_reactions": paid_reactions,
        "forwards": msg.forwards,
        "silent": msg.silent,
        "post": msg.post,
        "noforwards": msg.noforwards,
        "pinned": msg.pinned,
        "via_bot_id": msg.via_bot_id,
        "via_business_bot_id": msg.via_business_bot_id,
        "fwd_from_flag": msg.fwd_from is not None,
        "fwd_from_info": scrape_forward_from_info(msg.fwd_from)
        if msg.fwd_from
        else None,
        "photo": bool(msg.photo),
        "document": bool(msg.document),
        "web": bool(msg.web_preview),
        "audio": bool(msg.audio),
        "voice": bool(msg.voice),
        "video": bool(msg.video),
        "gif": bool(msg.gif),
        "geo": scrape_geo(msg),
        "poll": scrape_poll(msg),
        "comments": scrape_comments(msg),
    }

    # Save metadata to a JSON file
    with open(msg_dir / "meta.json", "w") as f:
        json.dump(msg_metadata, f, indent=4, ensure_ascii=False)


def scrape_text(msg: Message, msg_dir: Path) -> None:
    """
    Extracts and saves text from a message in two formats: raw and formatted.

    Args:
        msg (Message): The Telegram message object.
        msg_dir (Path): Directory path where text data will be saved.
    """
    if msg.raw_text:
        with open(msg_dir / "raw_text.txt", "w") as f:
            f.write(msg.raw_text)

    if msg.text:
        with open(msg_dir / "format_txt.txt", "w") as f:
            f.write(msg.text)


def scrape_url(msg: Message, msg_dir: Path) -> None:
    """
    Extracts and saves URLs from message entities, if present.

    Args:
        msg (Message): The Telegram message object.
        msg_dir (Path): Directory path where URLs will be saved.
    """
    urls = {}

    if msg.entities:
        for ent in msg.entities:
            if isinstance(ent, MessageEntityTextUrl):
                urls[ent.offset] = (ent.length, ent.url)

    with open(msg_dir / "urls.json", "w") as f:
        json.dump(urls, f, indent=4, ensure_ascii=False)


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


def scrape_msg(msg: Message, channel_url: str, msg_dir: Path) -> None:
    """
    Scrapes various attributes from a message and saves them to the specified directory.

    Args:
        msg (Message): The Telegram message object.
        channel_url (str): The URL of the Telegram channel.
        msg_dir (Path): Directory path where scraped data will be saved.
    """
    # Scrape reactions and message metadata
    paid_reactions = scrape_reactions(msg, msg_dir)
    scrape_metadata(msg, msg_dir, channel_url, paid_reactions)

    # Scrape text and URLs
    scrape_text(msg, msg_dir)
    scrape_url(msg, msg_dir)

    logging.info(f"Scraped message {msg.id} from channel: {channel_url}")
