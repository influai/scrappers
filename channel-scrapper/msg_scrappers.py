import json
import logging
from pathlib import Path

from telethon.tl.custom.message import Message
from telethon.tl.types import (
    MessageEntityTextUrl,
    MessageMediaDocument,
    MessageMediaPhoto,
    MessageMediaPoll,
    ReactionCustomEmoji,
    ReactionEmoji,
    ReactionPaid,
    MessageFwdHeader
)


def scrape_forward_from_info(fwd_from: MessageFwdHeader) -> dict:
    """
    scrape info about channel from which msg was forwarded and save this info
    
    https://tl.telethon.dev/constructors/message_fwd_header.html
    """
    info = {
        "channel_id": fwd_from.from_id.channel_id,
        "name": fwd_from.from_name,
        "channel_post": fwd_from.channel_post,
    }
    return info


def scrape_media(msg: Message) -> tuple[str, bool, bool, bool, bool]:
    """scrape the media flags - in future change"""
    media_flag = "True" if msg.media else "False"  # just for now
    img_flag, vid_flag, voice_flag, poll_flag = False, False, False, False
    if media_flag:
        if isinstance(msg.media, MessageMediaPhoto):
            img_flag = True
        elif isinstance(msg.media, MessageMediaDocument):
            if msg.media.video:
                vid_flag = True
            elif msg.media.voice:
                voice_flag = True
        elif isinstance(msg.media, MessageMediaPoll):
            poll_flag = True

    return media_flag, img_flag, vid_flag, voice_flag, poll_flag


def scrape_reactions(msg: Message, msg_dir: Path) -> int:
    """scrape reactions, save to the json and return the paid reactions"""
    paid_reactions = 0
    reactions_data = {"reaction_emoji": {}, "custom_emoji": {}}
    if msg.reactions:  # type MessageReactions
        for reaction_cnt in msg.reactions.results:
            if isinstance(reaction_cnt.reaction, ReactionPaid):
                paid_reactions = reaction_cnt.count
            elif isinstance(reaction_cnt.reaction, ReactionEmoji):
                emoji = reaction_cnt.reaction.emoticon  # emoji str
                count = reaction_cnt.count
                reactions_data["reaction_emoji"][emoji] = count
            elif isinstance(reaction_cnt.reaction, ReactionCustomEmoji):
                emoji_id = reaction_cnt.reaction.document_id  # emoji ID
                count = reaction_cnt.count
                reactions_data["custom_emoji"][emoji_id] = count

    # save reaction data
    with open(msg_dir / "reactions.json", "w") as f:
        json.dump(reactions_data, f, indent=4, ensure_ascii=False)

    return paid_reactions


def scrape_metadata(
    msg: Message,
    msg_dir: Path,
    channel_url: str,
    media_flags: tuple,
    paid_reactions: int,
) -> None:
    """
    scrape some meta from msg and save it along with media_flags and paid reactions
    
    https://docs.telethon.dev/en/stable/modules/custom.html#telethon.tl.custom.message.Message
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

        "fwd_from_flag": True if msg.fwd_from is not None else False,
        "fwd_from_info": None,  # if fwd_from_flag is True, this will contain the fwd from ch info

        "media": media_flags[0],
        "photo": media_flags[1],
        "video": media_flags[2],
        "voice": media_flags[3],
        "poll": media_flags[4],
    }

    # scrape info about "fwd_from" channel if so
    if msg.fwd_from:
        msg_metadata["fwd_from_info"] = scrape_forward_from_info(msg.fwd_from)

    # save metadata
    with open(msg_dir / "meta.json", "w") as f:
        json.dump(msg_metadata, f, indent=4, ensure_ascii=False)


def scrape_text(msg: Message, msg_dir: Path) -> None:
    """scrape text from message content and save (in future mb some preprocessing)"""
    if msg.message:
        with open(msg_dir / "message.txt", "w") as f:
            f.write(msg.message)


def scrape_url(msg: Message, msg_dir: Path) -> None:
    """extract and save URLs from the message entities (if any:))"""
    urls = {}
    if msg.entities:
        for ent in msg.entities:
            if isinstance(ent, MessageEntityTextUrl):
                urls[ent.offset] = (ent.length, ent.url)
    with open(msg_dir / "urls.json", "w") as f:
        json.dump(urls, f, indent=4, ensure_ascii=False)


def scrape_comments(msg: Message):
    """maybe need this"""
    pass


def scrape_msg(msg: Message, channel_url: str, msg_dir: Path) -> None:
    """
    scrape the various attributes from msg

    now: media flags, reactions
    """

    media_flags = scrape_media(msg)
    paid_reactions = scrape_reactions(msg, msg_dir)
    scrape_metadata(msg, msg_dir, channel_url, media_flags, paid_reactions)
    scrape_text(msg, msg_dir)
    scrape_url(msg, msg_dir)

    logging.info(f"channel: {channel_url}, message: {msg.id} scraped")
