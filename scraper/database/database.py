from datetime import datetime
from typing import Optional

from sqlalchemy import (
    ARRAY,
    JSON,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Double,
    Float,
    ForeignKey,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, relationship

Base = declarative_base()


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


class PostsMetadata(Base):
    """
    Contains metadata for posts.
    !!! Note that posts can be grouped together (e.g. a post with 2 photos = two objects with different id's that will have the same group_id).
    Max number of posts in group = 10.
    More info at `group_id` column comment.
    """

    __tablename__ = "posts_metadata"
    __table_args__ = (
        UniqueConstraint("channel_id", "post_id", name="uq_channel_post"),
        {
            "comment": """
    Contains metadata for posts.
    !!! Note that posts can be grouped together (e.g. a post with 2 photos = two objects with different id's that will have the same group_id).
    Max number of posts in group = 10.
    More info at `group_id` column comment.
    """
        },
    )

    id = Column(BigInteger, primary_key=True, comment="Unique record ID, auto handled by DB")
    channel_id = Column(
        BigInteger, nullable=False, comment="ID of the channel this post belongs to"
    )
    post_id = Column(
        BigInteger,
        nullable=False,
        comment="ID of the post within the channel. All posts have unique IDs inside one channel",
    )
    group_id = Column(
        BigInteger,
        nullable=True,
        comment="Multiple posts with the same `group_id` indicate an album or media group. "
        + "IMPORTANT: only 1 post in a group has reactions and comments, so we need somehow aggregate all posts within the same group. "
        + "`group_id` is unique only within ONE channel. Max number of posts in group = 10. "
        + "If this field = None, it means it is a single post that no longer includes any other information, so no need to worry about aggregation",
    )
    post_date = Column(
        DateTime(timezone=True), nullable=False, comment="Datetime post was published"
    )
    scrape_date = Column(
        DateTime(timezone=True), nullable=False, comment="Datetime post data was scraped"
    )

    # Relationships to other tables
    posts_flags = relationship("PostsFlags", back_populates="posts_metadata", uselist=False)
    posts_metrics = relationship("PostsMetrics", back_populates="posts_metadata", uselist=False)
    posts_content = relationship("PostsContent", back_populates="posts_metadata", uselist=False)
    forwards = relationship("Forwards", back_populates="posts_metadata", uselist=False)


class PostsFlags(Base):
    """
    Contains flags for posts
    """

    __tablename__ = "posts_flags"
    __table_args__ = ({"comment": "Contains flags for posts"},)

    id = Column(
        BigInteger,
        ForeignKey("posts_metadata.id"),
        primary_key=True,
        comment="Post ID (FK to posts_metadata)",
    )
    is_post = Column(
        Boolean,
        nullable=True,
        comment="Whether the record is a post (True) or a notification (False)",
    )
    silent = Column(
        Boolean, nullable=True, comment="Whether the post is silent (no notifications sent)"
    )
    noforwards = Column(
        Boolean, nullable=True, comment="Whether forwarding of this post is disabled"
    )
    pinned = Column(Boolean, nullable=True, comment="Whether the post is pinned")
    fwd_from_flag = Column(
        Boolean, nullable=False, comment="Flag to indicate if the post is forwarded"
    )

    photo = Column(Boolean, nullable=False, comment="Whether the post contains a photo")
    document = Column(Boolean, nullable=False, comment="Whether the post contains a document")
    web = Column(Boolean, nullable=False, comment="Whether the post contains a web preview")
    audio = Column(Boolean, nullable=False, comment="Whether the post contains an audio file")
    voice = Column(Boolean, nullable=False, comment="Whether the post contains a voice note")
    video = Column(Boolean, nullable=False, comment="Whether the post contains a video")
    gif = Column(Boolean, nullable=False, comment="Whether the post contains a GIF")

    # Relationship to posts_metadata
    posts_metadata = relationship("PostsMetadata", back_populates="posts_flags")


class PostsMetrics(Base):
    """
    Contains metrics for posts
    """

    __tablename__ = "posts_metrics"
    __table_args__ = ({"comment": "Contains metrics for posts"},)

    id = Column(
        BigInteger,
        ForeignKey("posts_metadata.id"),
        primary_key=True,
        comment="Post ID (FK to posts_metadata)",
    )
    views = Column(
        BigInteger,
        nullable=True,
        comment="Number of views on the post. Null if the message is just notification (f.e. 'channel created', 'stream started')",
    )
    forwards = Column(BigInteger, nullable=True, comment="Number of forwards of the post")
    comments = Column(
        BigInteger,
        nullable=True,
        comment="The number of comments on this post. Can be None if comments are not allowed",
    )
    paid_reactions = Column(
        BigInteger, nullable=True, comment="Number of paid reactions, i.e. stars given to the post"
    )
    standard_reactions = Column(
        JSON,
        nullable=True,
        comment="Data about reactions of post. Format: dict where keys are the Unicode characters"
        + " and values are number of this type of reactions on post."
        + "F.e.: {'😀': 10, '🫠': 52}",
    )
    custom_reactions = Column(
        JSON,
        nullable=True,
        comment="This also data about reactions, but this contains the custom reactions,"
        + "which are not included in Unicode and don't have Unicode codes, instead they have reaction ID,"
        + " which are keys of the dictionary and the values are number of reactions of such ID."
        + " F.e.: {'345678': 10, '987654': 52}",
    )

    # Relationship to posts_metadata
    posts_metadata = relationship("PostsMetadata", back_populates="posts_metrics")


class PostsContent(Base):
    """
    Contains content details for posts.
    Record is added only if at least one of the attributes has data (i.e. not None and not empty).
    """

    __tablename__ = "posts_content"
    __table_args__ = (
        {
            "comment": """
    Contains content details for posts.
    Record is added only if at least one of the attributes has data (i.e. not None and not empty).
    """
        },
    )

    id = Column(
        BigInteger,
        ForeignKey("posts_metadata.id"),
        primary_key=True,
        comment="Post ID (FK to posts_metadata)",
    )
    raw_text = Column(
        String,
        nullable=True,
        comment="The raw post text, ignoring any formatting, i.e. without any entities in it."
        + " F.e. if original post text: 'hello, **world** **[гугл](https://google.com)', then the raw text will be: 'hello, world'"
        + " Can be None if it is ServiceMessage.",
    )
    urls = Column(
        ARRAY(String),
        nullable=True,
        comment="List of URLs extracted from the post content. Can be None if post doesnt contains any URLs",
    )
    geo = Column(
        ARRAY(Float),
        nullable=True,
        comment="Geolocation data (longitude, latitude) if attached to the post, or None if no geo data is present",
    )
    poll = Column(
        JSON,
        nullable=True,
        comment="Poll data from post, including the question, answers, and total voters."
        + "Represents as dict with 3 keys: 'question': str, 'answers': list(str), 'total_voters': int. "
        + "Note, that this entity is None if there is no poll in this post",
    )
    via_bot_id = Column(BigInteger, nullable=True, comment="ID of the bot that posted the message")
    via_business_bot_id = Column(
        BigInteger, nullable=True, comment="ID of the business bot associated with the post"
    )

    # Relationship to posts_metadata
    posts_metadata = relationship("PostsMetadata", back_populates="posts_content")


class Forwards(Base):
    """
    Represents forwarded posts between Telegram channels.
    """

    __tablename__ = "forwards"
    __table_args__ = ({"comment": "Tracks forwards of posts from one channel to another"},)

    id = Column(
        BigInteger,
        ForeignKey("posts_metadata.id"),
        primary_key=True,
        comment="Post ID (FK to posts_metadata)",
    )

    from_ch_id = Column(
        BigInteger,
        nullable=False,
        comment="ID of the source channel (i.e. the channel wherein this post was 'originally' posted)",
    )

    from_post_id = Column(
        BigInteger,
        nullable=True,
        comment="ID of the source post in source channel (if not available = None)",
    )

    # Relationship to posts_metadata
    posts_metadata = relationship("PostsMetadata", back_populates="forwards")


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
