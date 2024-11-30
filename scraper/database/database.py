from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Double,
    Float,
    ForeignKey,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Channels(Base):
    """
    Table to store primary information about Telegram channels
    """

    __tablename__ = "channels"
    __table_args__ = {"comment": "Table to store primary information about Telegram channels"}

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, comment="Unique Telegram channel ID"
    )
    name: Mapped[str] = mapped_column(
        String,
        nullable=False,
        comment="Channel's name, which uses in url (f.e. `channel_name`)",
    )
    title: Mapped[str] = mapped_column(
        String,
        nullable=False,
        comment="Channel title, that's what we see in TG app, f.e. 'Лучшие ставки!!!'",
    )
    participants: Mapped[int] = mapped_column(
        BigInteger, nullable=False, comment="Number of participants in the channel"
    )
    last_pinned_msg_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, nullable=True, comment="ID of the last pinned message, if any"
    )
    about: Mapped[Optional[str]] = mapped_column(
        String, nullable=True, comment="Description or 'about' section of the channel"
    )

    def __repr__(self) -> str:
        return f"Channels(id={self.id!r}, name={self.name!r}, participants={self.participants!r})"


class Similars(Base):
    """
    Stores information about similar channels suggested by Telegram Premium.
    Usually about 100 similar channels are provided for channel.
    """

    __tablename__ = "similars"
    __table_args__ = (
        UniqueConstraint("base_channel_id", "similar_channel_id", name="uq_base_similar"),
        {
            "comment": "Data about similar channels provided by Telegram Premium."
            + "Usually about 100 similar channels are provided for channel"
        },
    )

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, comment="Unique record ID, automatically handled by DB not me"
    )
    base_channel_id: Mapped[int] = mapped_column(
        ForeignKey("channels.id"),
        comment="ID of the base channel to which similar channels are provided",
    )
    similar_channel_id: Mapped[int] = mapped_column(BigInteger, comment="ID of the similar channel")
    similar_channel_name: Mapped[Optional[str]] = mapped_column(
        String,
        nullable=True,
        comment="Name of the similar channel, which uses in url (f.e. @name, but without @!)",
    )
    similar_channel_title: Mapped[Optional[str]] = mapped_column(
        String, nullable=True, comment="Title of the similar channel"
    )

    def __repr__(self) -> str:
        return f"Similars(base_channel={self.base_channel_id!r}, similar_channel={self.similar_channel_id!r})"


class Posts(Base):
    """
    Represents individual posts from Telegram channels.
    """

    __tablename__ = "posts"
    __table_args__ = (
        UniqueConstraint("channel_id", "post_id", name="uq_channel_post"),
        {"comment": "Contains data about posts published in Telegram channels"},
    )

    id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, comment="Unique record ID, automatically handled by DB not me"
    )
    channel_id: Mapped[int] = mapped_column(
        ForeignKey("channels.id"), comment="ID of the channel this post belongs to"
    )
    post_id: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        comment="ID of the post in channel, all posts have unique IDs inside one channel",
    )

    post_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="Datetime when the post was published"
    )
    scrape_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="Datetime when the post data was scraped"
    )

    views: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        nullable=True,
        comment="Number of views on the post. Null if the message is just notification (f.e. 'channel created', 'stream started')",
    )
    paid_reactions: Mapped[Optional[int]] = mapped_column(
        BigInteger, nullable=True, comment="Number of paid reactions, i.e. stars given to the post"
    )
    forwards: Mapped[Optional[int]] = mapped_column(
        BigInteger, nullable=True, comment="Number of forwards of this post"
    )
    comments: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        nullable=True,
        comment="The number of comments on this post. Can be None if comments are not allowed.",
    )

    silent: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True, comment="Whether the post is silent (no notifications sent)"
    )
    is_post: Mapped[Optional[bool]] = mapped_column(
        Boolean,
        nullable=True,
        comment="Whether the record is a post (True) or a notification (False)",
    )
    noforwards: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True, comment="Whether forwarding of this post is disabled"
    )
    pinned: Mapped[Optional[bool]] = mapped_column(
        Boolean, nullable=True, comment="Whether the post is pinned in the channel"
    )

    via_bot_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, nullable=True, comment="ID of the bot that posted the message"
    )
    via_business_bot_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, nullable=True, comment="ID of the business bot associated with the post"
    )

    fwd_from_flag: Mapped[bool] = mapped_column(
        Boolean, nullable=False, comment="Flag to indicate if the post is forwarded"
    )

    photo: Mapped[bool] = mapped_column(
        Boolean, nullable=False, comment="Flag to indicate if the post contains a photo"
    )
    document: Mapped[bool] = mapped_column(
        Boolean, nullable=False, comment="Flag to indicate if the post contains a document"
    )
    web: Mapped[bool] = mapped_column(
        Boolean, nullable=False, comment="Flag to indicate if the post contains a web preview"
    )
    audio: Mapped[bool] = mapped_column(
        Boolean, nullable=False, comment="Flag to indicate if the post contains an audio file"
    )
    voice: Mapped[bool] = mapped_column(
        Boolean, nullable=False, comment="Flag to indicate if the post contains a voice note"
    )
    video: Mapped[bool] = mapped_column(
        Boolean, nullable=False, comment="Flag to indicate if the post contains a video"
    )
    gif: Mapped[bool] = mapped_column(
        Boolean, nullable=False, comment="Flag to indicate if the post contains a GIF"
    )

    geo: Mapped[Optional[Tuple[float, float]]] = mapped_column(
        ARRAY(Float, dimensions=1),
        nullable=True,
        comment="Geolocation data (longitude, latitude) if attached to the post, or None if no geo data is present.",
    )
    poll: Mapped[Optional[Dict[str, Union[str, List[str], int]]]] = mapped_column(
        JSON,
        nullable=True,
        comment="Poll data from post, including the question, answers, and total voters."
        + "Represents as dict with 3 keys: 'question': str, 'answers': list(str), 'total_voters': int. "
        + "Note, that this entity is None if there is no poll in this post.",
    )

    standard_reactions: Mapped[Optional[Dict[str, int]]] = mapped_column(
        JSON,
        nullable=True,
        comment="Data about reactions of post. Format: dict where keys are the Unicode characters"
        + " and values are number of this type of reactions on post."
        + "F.e.: {'😀': 10, '🫠': 52}",
    )
    custom_reactions: Mapped[Optional[Dict[int, int]]] = mapped_column(
        JSON,
        nullable=True,
        comment="This also data about reactions, but this contains the custom reactions,"
        + "which are not included in Unicode and don't have Unicode codes, instead they have reaction ID,"
        + " which are keys of the dictionary and the values are number of reactions of such ID."
        + " F.e.: {'345678': 10, '987654': 52}",
    )

    raw_text: Mapped[Optional[str]] = mapped_column(
        String,
        nullable=True,
        comment="The raw post text, ignoring any formatting, i.e. without any entities in it."
        + " F.e. if original post text: 'hello, **world** **[гугл](https://google.com)', then the raw text will be: 'hello, world'"
        + " Can be None if it is ServiceMessage.",
    )
    
    urls: Mapped[Optional[List[str]]] = mapped_column(
        ARRAY(String, dimensions=1),
        nullable=True,
        comment="List of URLs extracted from the post content. Can be None if post doesnt contains any URLs",
    )

    def __repr__(self) -> str:
        return (
            f"Posts(channel id={self.channel_id!r}, post id={self.post_id!r}, views={self.views!r})"
        )


class Forwards(Base):
    """
    Represents forwarded posts between Telegram channels.
    """

    __tablename__ = "forwards"
    __table_args__ = (
        UniqueConstraint("from_ch_id", "from_post_id", "to_ch_id", "to_post_id", name="uq_forward"),
        {"comment": "Tracks forwards of posts from one channel to another"},
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        comment="Unique forward record ID, automatically handled by DB not me",
    )
    from_ch_id: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        comment="ID of the source channel (i.e. the channel wherein this post was 'originally' posted)",
    )
    from_post_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        nullable=True,
        comment="ID of the source post in source channel (if not available = None)",
    )
    to_ch_id: Mapped[int] = mapped_column(
        ForeignKey("channels.id"),
        nullable=False,
        comment="ID of the target channel (i.e. channel which make forward)",
    )
    to_post_id: Mapped[int] = mapped_column(
        BigInteger, nullable=False, comment="ID of the target post in target channel"
    )

    def __repr__(self) -> str:
        return f"Forwards(id={self.id!r}, from_ch_id={self.from_ch_id!r}, from_post_id={self.from_post_id!r}, to_ch_id={self.to_ch_id!r}, to_post_id={self.to_post_id!r})"


class Peers(Base):
    """
    Stores peer information (ID and access hash) for Telegram channels for corrsponding scraper.
    This info is needed for reducing the Telegram API calls
    """

    __tablename__ = "peers"
    __table_args__ = {"comment": "Contains channel peers (ID, access_hash) for efficient API usage"}

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        comment="Unique forward record ID, automatically handled by DB not me",
    )
    scraper_id: Mapped[int] = mapped_column(
        BigInteger, nullable=False, comment="Scraper ID associated with the peer data"
    )
    channel_name: Mapped[str] = mapped_column(String, nullable=False, comment="Name of the channel")
    channel_id: Mapped[int] = mapped_column(
        BigInteger, nullable=False, comment="Telegram ID of the channel"
    )
    access_hash: Mapped[int] = mapped_column(
        BigInteger, nullable=False, comment="Access hash for the channel"
    )

    def __repr__(self) -> str:
        return (
            f"Peers(id={self.id!r}, scraper_id={self.scraper_id!r}), channel_id={self.channel_id!r}"
        )


class Runs(Base):
    """
    Logs scraping runs for tracking data collection progress.
    """

    __tablename__ = "runs"
    __table_args__ = {"comment": "Tracks data collection runs for channels"}

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        comment="Unique forward record ID, automatically handled by DB not me",
    )
    channel_id: Mapped[int] = mapped_column(
        ForeignKey("channels.id"), comment="ID of channel, which was scraped"
    )
    from_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, comment="Time cutoff from which posts were scraped"
    )
    scrape_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="Date and time when this scraping process was started",
    )
    posts_scraped: Mapped[int] = mapped_column(
        BigInteger, comment="Number of posts which was successfully scraped"
    )
    exec_time: Mapped[float] = mapped_column(
        Double, comment="Execution time of this process, in seconds"
    )

    def __repr__(self) -> str:
        return f"Runs(id={self.id!r}, channel_id={self.channel_id!r}, scrape_date={self.scrape_date!r})"
