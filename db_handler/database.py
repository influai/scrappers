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
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Channel(Base):
    __tablename__ = "channel"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    url: Mapped[str] = mapped_column(String, nullable=False)
    participants: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_pinned_msg_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    about: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    posts: Mapped[List["Post"]] = relationship(back_populates="channel")

    def __repr__(self) -> str:
        return f"Channel(id={self.id!r}, name={self.name!r}, participants={self.participants!r})"


class Similars(Base):
    __tablename__ = "similars"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    base_channel_id: Mapped[int] = mapped_column(ForeignKey("channel.id"))
    similar_channel_id: Mapped[int] = mapped_column(BigInteger)
    similar_channel_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    similar_channel_title: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "base_channel_id", "similar_channel_id", name="uq_base_similar"
        ),
    )

    def __repr__(self) -> str:
        return f"Similars(id={self.id!r}, base_channel={self.base_channel_id!r}, similar_channel={self.similar_channel_id!r})"


class Post(Base):
    __tablename__ = "post"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channel.id"))
    channel: Mapped["Channel"] = relationship("Channel", back_populates="posts")
    url: Mapped[str] = mapped_column(String, nullable=False)

    post_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    scrape_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )

    views: Mapped[Optional[int]] = mapped_column(
        BigInteger, nullable=True
    )  # Null if the message is just notification (f.e. "channel created", "stream started")
    paid_reactions: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    forwards: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    comments: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)

    silent: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    is_post: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    noforwards: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    pinned: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    via_bot_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    via_business_bot_id: Mapped[Optional[int]] = mapped_column(
        BigInteger, nullable=True
    )

    fwd_from_flag: Mapped[bool] = mapped_column(Boolean, nullable=False)
    fwd_from_info: Mapped[Optional["Forwards"]] = relationship(
        "Forwards", uselist=False, back_populates="to_post"
    )

    photo: Mapped[bool] = mapped_column(Boolean, nullable=False)
    document: Mapped[bool] = mapped_column(Boolean, nullable=False)
    web: Mapped[bool] = mapped_column(Boolean, nullable=False)
    audio: Mapped[bool] = mapped_column(Boolean, nullable=False)
    voice: Mapped[bool] = mapped_column(Boolean, nullable=False)
    video: Mapped[bool] = mapped_column(Boolean, nullable=False)
    gif: Mapped[bool] = mapped_column(Boolean, nullable=False)

    geo: Mapped[Optional[Tuple[float, float]]] = mapped_column(
        ARRAY(Float, dimensions=1), nullable=True
    )
    poll: Mapped[Optional[Dict[str, Union[str, List[str], int]]]] = mapped_column(
        JSON, nullable=True
    )

    standard_reactions: Mapped[Optional[Dict[str, int]]] = mapped_column(
        JSON, nullable=True
    )
    custom_reactions: Mapped[Optional[Dict[int, int]]] = mapped_column(
        JSON, nullable=True
    )

    raw_text: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    format_text: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    urls: Mapped[Optional[Dict[str, Union[int, str]]]] = mapped_column(
        JSON, nullable=True
    )

    def __repr__(self) -> str:
        return f"Post(id={self.id!r}, url={self.url!r}, views={self.views!r})"


class Forwards(Base):
    __tablename__ = "forwards"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    from_ch_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    from_post_id: Mapped[Optional[int]] = mapped_column(BigInteger, nullable=True)
    to_ch_id: Mapped[int] = mapped_column(ForeignKey("channel.id"), nullable=False)
    to_post_id: Mapped[int] = mapped_column(ForeignKey("post.id"), nullable=False)

    to_post: Mapped["Post"] = relationship("Post", back_populates="fwd_from_info")

    def __repr__(self) -> str:
        return f"Forwards(id={self.id!r}, from_ch_id={self.from_ch_id!r}, to_ch_id={self.to_ch_id!r})"


class Runs(Base):
    __tablename__ = "runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    channel_id: Mapped[int] = mapped_column(ForeignKey("channel.id"))
    channel_url: Mapped[str] = mapped_column(String)
    from_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    to_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    scrape_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    posts_scraped: Mapped[int] = mapped_column(BigInteger)
    exec_time: Mapped[float] = mapped_column(Double)

    def __repr__(self) -> str:
        return f"Runs(id={self.id!r}, channel_id={self.channel_id!r}, scrape_date={self.scrape_date!r})"
