import logging
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Union

from database.database import (
    Channels,
    Forwards,
    Peers,
    PostsContent,
    PostsFlags,
    PostsMetadata,
    PostsMetrics,
    Runs,
    Similars,
)
from database.session import get_database_session
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from telethon import TelegramClient, functions
from telethon.errors import (
    AuthKeyDuplicatedError,
    AuthKeyPermEmptyError,
    ChannelInvalidError,
    ChannelPrivateError,
    ChannelPublicGroupNaError,
    ChatIdInvalidError,
    FloodWaitError,
    PeerIdInvalidError,
    SessionPasswordNeededError,
    TimeoutError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.tl.custom.message import Message
from telethon.tl.types import (
    GeoPoint,
    MessageEntityTextUrl,
    MessageEntityUrl,
    ReactionCustomEmoji,
    ReactionEmoji,
    ReactionPaid,
)
from telethon.types import InputPeerChannel

from scraper.flood_error_caretaker import FloodCaretaker


class ChannelScraper:
    """
    Represents scraper for channel level.

    Gets the database session and write new data (channel peers, channel metadata, similar channels)
    in transactional manner (commits iff no errors occurs).
    """

    def __init__(self, tg_client: TelegramClient, scraper_id: int):
        self.tg_client = tg_client
        self.scraper_id = scraper_id
        self.flood_care = FloodCaretaker()

    def check_channel_name(self, channel_name: str) -> bool:
        """
        Simple check for channel name using regexp
        """
        return bool(re.match(r"^\w+$", channel_name))

    async def get_peer(self, channel_name: str) -> InputPeerChannel:
        """
        Get the channel peer to corresponding scraper.

        The function works as follows:
        1) Checks if peer data exists in the database and retrieves it.
        2) If it doesn't exist, calls `get_input_entity` (which may have a high timeout) to fetch the data.

        Note: The access_hash is unique to each account (scraper).
        """
        channel = None  # Channel peer will be stored here

        # Search for channel peer in Peers table
        with get_database_session() as db_session:
            result = db_session.execute(
                select(Peers).where(
                    Peers.channel_name == channel_name, Peers.scraper_id == self.scraper_id
                )
            )
            peer = result.scalar_one_or_none()
            if peer:
                channel = InputPeerChannel(channel_id=peer.channel_id, access_hash=peer.access_hash)
        if channel is not None:
            logging.info(f"Successfully loaded peer for channel @{channel_name}")
            return channel

        # If peer not found, try to call ResolveUsername
        logging.info(f"Peer not found for @{channel_name}")
        self.flood_care.check()  # Check the FloodWaitError state and wait setted delay
        # Fetch the peer data from Telegram API
        logging.info("Calling ResolveUsername")
        try:
            channel = await self.tg_client.get_input_entity(channel_name)
        except FloodWaitError as fwe:
            self.flood_care.add_fwe(fwe.seconds)
            raise  # propogate the error forward
        assert isinstance(channel, InputPeerChannel)

        # Save new peer data to the database
        with get_database_session() as db_session:
            new_peer = Peers(
                scraper_id=self.scraper_id,
                channel_name=channel_name,
                channel_id=channel.channel_id,
                access_hash=channel.access_hash,
            )
            db_session.add(new_peer)
        logging.info("Successfully fetched and added new peer to the database")

        return channel

    async def scrape_channel_metadata(self, channel: InputPeerChannel, channel_name: str) -> None:
        """
        Get full channel info and upsert into database
        """
        full = await self.tg_client(functions.channels.GetFullChannelRequest(channel))
        full_channel = full.full_chat

        channel_data = {
            "id": full_channel.id,
            "title": full.chats[0].title,
            "name": channel_name,
            "participants": full_channel.participants_count,
            "last_pinned_msg_id": full_channel.pinned_msg_id,
            "about": full_channel.about,
        }

        with get_database_session() as db_session:
            stmt = insert(Channels).values(channel_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "title": stmt.excluded.title,
                    "name": stmt.excluded.name,
                    "participants": stmt.excluded.participants,
                    "last_pinned_msg_id": stmt.excluded.last_pinned_msg_id,
                    "about": stmt.excluded.about,
                },
            )
            db_session.execute(stmt)
        logging.info(f"Successfully scraped metadata for channel @{channel_name}.")

    async def scrape_similar_channels(self, channel: InputPeerChannel, channel_name: str) -> None:
        """
        Scrapes the list of similar Telegram channels and adds them to the database.
        """
        result = await self.tg_client(functions.channels.GetChannelRecommendationsRequest(channel))

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

            # Upsert data into database
            with get_database_session() as db_session:
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

    def save_run_info(
        self,
        channel_id: int,
        from_date: datetime,
        scrape_date: datetime,
        posts_scraped: int,
        start_time: float,
    ) -> None:
        """
        Save the run information (timestamps, post counts, etc.) to DB
        """
        with get_database_session() as db_session:
            db_session.add(
                Runs(
                    channel_id=channel_id,
                    from_date=from_date,
                    scrape_date=scrape_date,
                    posts_scraped=posts_scraped,
                    exec_time=time.time() - start_time,
                )
            )

    async def scrape(self, channel_name: str, from_date: datetime) -> bool:
        """
        Main scraping method, calls scraping all kinds of data at the channel level.
        All datadase interactions splitted on transactions and wrapped with context manager 'get_database_session',
        which provides transactional scope around all code inside it.

        Returns:
            bool: Acknowledge flag (True for success or invalid task, False for retryable failure).
        """
        scrape_date = datetime.now(tz=timezone.utc)
        start_time = time.time()
        try:
            logging.info(f"Starting scraping task for @{channel_name} from {from_date}")

            if not self.check_channel_name(channel_name):
                logging.warning("Channel name is invalid")
                return True

            channel: InputPeerChannel = await self.get_peer(channel_name)

            # Check if channel was already parsed, then skip it
            # with get_database_session() as db_session:
            #     result = db_session.execute(
            #         select(Runs).where(Runs.channel_id == channel.channel_id)
            #     )
            #     prev_run_info = result.first()
            #     if prev_run_info:
            #         logging.info(f"Channel @{channel_name} was already parsed, so removing this task")
            #         return True

            await self.scrape_channel_metadata(channel, channel_name)
            await self.scrape_similar_channels(channel, channel_name)

            # Scrape posts
            post_scraper = PostScraper(channel, channel_name)
            posts_scraped: int = await post_scraper.run(self.tg_client, from_date)

            # Save the run information (timestamps, post counts, etc.) to DB
            self.save_run_info(
                channel.channel_id, from_date, scrape_date, posts_scraped, start_time
            )

            logging.info(
                f"Successfully finished scraping task for @{channel_name} "
                f"from {from_date} to present. Total posts scraped: {posts_scraped}"
            )
            return True

        except (
            SQLAlchemyError,
            AuthKeyPermEmptyError,
            SessionPasswordNeededError,
            TimeoutError,
            AuthKeyDuplicatedError,
            PeerIdInvalidError,
            FloodWaitError,
        ) as scraper_bound_err:
            logging.error(
                "Scraper-side error occurred. Task will be retried. Error details:",
                exc_info=True,
            )
            return False

        except (
            UsernameInvalidError,
            UsernameNotOccupiedError,
            ValueError,
            ChannelInvalidError,
            ChannelPrivateError,
            ChannelPublicGroupNaError,
            ChatIdInvalidError,
            AssertionError,
        ) as task_bound_err:
            logging.warning(
                "Task-related error occurred. Removing task from queue. Error details:",
                exc_info=True,
            )
            return True

        except Exception as unk_err:
            logging.error(
                "An unknown error occurred. Task will be retried. Error details:", exc_info=True
            )
            return False


class PostScraper:
    """
    Represents post scraper.

    Scrapes various types of data from all posts from specified date.
    Also manages the database transactions.
    """

    def __init__(self, channel: InputPeerChannel, channel_name: str) -> None:
        self.channel = channel
        self.channel_name = channel_name

    def scrape_geo(self, post: Message) -> tuple[float, float] | None:
        """
        Extracts the longitude and latitude from a message's geo location (if available).

        Returns tuple of longitude and latitude as floats, or None if no geo data is present.
        """
        if isinstance(post.geo, GeoPoint):
            return (post.geo.long, post.geo.lat)
        if post.venue and isinstance(post.venue.geo, GeoPoint):
            return (post.venue.geo.long, post.venue.geo.lat)
        return None

    def scrape_poll(self, post: Message) -> None | Dict[str, Union[str, List[str], int]]:
        """
        Extracts poll information from a message, including the question, answers, and total voters.

        Returns dict containing the poll question, list of answers, and total voters, or None if no poll.
        """
        if not post.poll:
            return None

        poll_info = {
            "question": post.poll.poll.question.text,
            "answers": [answer.text.text for answer in post.poll.poll.answers],
            "total_voters": post.poll.results.total_voters,
        }
        return poll_info

    def scrape_urls(self, post: Message) -> List[str]:
        """
        Extracts URLs from message entities. If post doesnt contain any URL return None.
        """
        urls = []

        if post.entities:
            for ent in post.entities:
                if isinstance(ent, MessageEntityTextUrl):
                    urls.append(ent.url)
                elif isinstance(ent, MessageEntityUrl):
                    urls.append(post.raw_text[ent.offset : ent.offset + ent.length])
        return urls
    
    def scrape_forward(self, post: Message) -> Dict:
        """
        Extracts information about the original channel a message was forwarded from, if it is.
        """
        try:
            return {
                "from_ch_id": post.fwd_from.from_id.channel_id,
                "from_post_id": post.fwd_from.channel_post,
            }
        except Exception:
            return {
                "from_ch_id": None,
                "from_post_id": None,
            }
        
    def scrape_reactions(self, post: Message) -> Tuple[int, dict, dict]:
        """
        Scrapes 3 types of reactions from post:
        1. Number of paid reactions, i.e. stars given to the pose
        2. Standard reactions - dict where keys are the Unicode characters and values are number of
        this type of reactions on post
        3. This also data about reactions, but this contains the custom reactions,
        which are not included in Unicode and don't have Unicode codes, instead they have reaction ID,
        which are keys of the dictionary and the values are number of reactions of such ID.
        """
        paid_reactions = 0
        stardard_reactions = {}
        custom_reactions = {}

        if post.reactions:
            for reaction_cnt in post.reactions.results:
                if isinstance(reaction_cnt.reaction, ReactionPaid):
                    paid_reactions = reaction_cnt.count
                elif isinstance(reaction_cnt.reaction, ReactionEmoji):
                    emoji = reaction_cnt.reaction.emoticon
                    stardard_reactions[emoji] = reaction_cnt.count
                elif isinstance(reaction_cnt.reaction, ReactionCustomEmoji):
                    emoji_id = reaction_cnt.reaction.document_id
                    custom_reactions[emoji_id] = reaction_cnt.count

        return paid_reactions, stardard_reactions, custom_reactions

    def scrape_post(self, post: Message) -> Dict:
        """
        Extracts and organizes data about a post from the Telethon Message object.

        Returns a dictionary representing the data for insertion into the database.
        """
        paid_reactions, standard_reactions, custom_reactions = self.scrape_reactions(post)

        post_data = {
            "posts_metadata": {
                "channel_id": self.channel.channel_id,
                "post_id": post.id,
                "group_id": post.grouped_id,
                "post_date": post.date,
                "scrape_date": datetime.now(tz=timezone.utc),
            },
            "posts_flags": {
                "is_post": post.post,
                "silent": post.silent,
                "noforwards": post.noforwards,
                "pinned": post.pinned,
                "fwd_from_flag": post.fwd_from is not None,
                "photo": bool(post.photo),
                "document": bool(post.document),
                "web": bool(post.web_preview),
                "audio": bool(post.audio),
                "voice": bool(post.voice),
                "video": bool(post.video),
                "gif": bool(post.gif),
            },
            "posts_metrics": {
                "views": post.views,
                "forwards": post.forwards,
                "comments": post.replies.replies if post.replies else None,
                "paid_reactions": paid_reactions,
                "standard_reactions": standard_reactions,
                "custom_reactions": custom_reactions,
            },
            "posts_content": {
                "raw_text": post.raw_text,
                "urls": self.scrape_urls(post),
                "geo": self.scrape_geo(post),
                "poll": self.scrape_poll(post),
                "via_bot_id": post.via_bot_id,
                "via_business_bot_id": post.via_business_bot_id,
            },
            "forwards": self.scrape_forward(post),
        }

        return post_data

    async def scrape_posts_batch(self, posts: List[Message]) -> None:
        """
        Iteratively process all posts, removing posts, which raises errors while trying to scrape.
        Successful posts are inserted into the database.
        """
        db_posts_metadata = []
        db_posts_flags = []
        db_posts_metrics = []
        db_posts_content = []
        db_forwards = []

        for post in posts:
            # If any error encountered while processing a single post - skip that post
            try:
                post_data = self.scrape_post(post)

                db_posts_metadata.append(post_data["posts_metadata"])
                db_posts_flags.append(post_data["posts_flags"])
                db_posts_metrics.append(post_data["posts_metrics"])
                db_posts_content.append(post_data["posts_content"])
                db_forwards.append(post_data["forwards"])
            except Exception as e:
                logging.error(f"Error processing post {post.id}: {e}")
                continue

        # Bulk upsert data
        with get_database_session() as db_session:
            if db_posts_metadata:
                metadata_stmt = insert(PostsMetadata).values(db_posts_metadata)
                metadata_stmt = metadata_stmt.on_conflict_do_update(
                    constraint="uq_channel_post",
                    set_={col: metadata_stmt.excluded[col] for col in ["group_id", "post_date", "scrape_date"]},
                ).returning(PostsMetadata.id, PostsMetadata.channel_id, PostsMetadata.post_id)
                result = db_session.execute(metadata_stmt)

                # Map (channel_id, post_id) to the generated ID
                id_map = {(row.channel_id, row.post_id): row.id for row in result.fetchall()}

                # Update dependent data with correct IDs
                for i in range(len(db_posts_metadata)):
                    metadata = db_posts_metadata[i]
                    id = id_map[(metadata["channel_id"], metadata["post_id"])]

                    db_posts_flags[i]["id"] = id
                    db_posts_metrics[i]["id"] = id
                    db_posts_content[i]["id"] = id
                    db_forwards[i]["id"] = id

                # Now upsert these tables

                # posts_flags
                flags_stmt = insert(PostsFlags).values(db_posts_flags)
                flags_stmt = flags_stmt.on_conflict_do_update(
                    constraint="posts_flags_pkey",
                    set_={
                        col: flags_stmt.excluded[col]
                        for col in db_posts_flags[0].keys()
                        if col != "id"
                    },
                )
                db_session.execute(flags_stmt)

                # posts_metrics
                metrics_stmt = insert(PostsMetrics).values(db_posts_metrics)
                metrics_stmt = metrics_stmt.on_conflict_do_update(
                    constraint="posts_metrics_pkey",
                    set_={
                        col: metrics_stmt.excluded[col]
                        for col in db_posts_metrics[0].keys()
                        if col != "id"
                    },
                )
                db_session.execute(metrics_stmt)

                # posts_content
                # before upsert, remove all empty records
                db_posts_content = [
                    content for content in db_posts_content if any(list(content.values())[:-1])
                ]
                if db_posts_content:
                    content_stmt = insert(PostsContent).values(db_posts_content)
                    content_stmt = content_stmt.on_conflict_do_update(
                        constraint="posts_content_pkey",
                        set_={
                            col: content_stmt.excluded[col]
                            for col in db_posts_content[0].keys()
                            if col != "id"
                        },
                    )
                    db_session.execute(content_stmt)

                # forwards
                # before upsert, remove all empty records
                db_forwards = [
                    fwd_rec for fwd_rec in db_forwards if any(list(fwd_rec.values())[:-1])
                ]
                if db_forwards:
                    forward_stmt = insert(Forwards).values(db_forwards)
                    forward_stmt = forward_stmt.on_conflict_do_nothing(constraint="forwards_pkey")
                    db_session.execute(forward_stmt)

        logging.info(f"Successfully upserted {len(db_posts_metadata)} posts into the database.")

    async def run(self, tg_client: TelegramClient, from_date: datetime, limit: int = 20000) -> int:
        """
        Starts scraping process, iteratively process posts from channel from specified date to present time.
        Has limit for number of posts to scrape from date (scrapes only `limit` posts from current time).

        Calc the default value for limit for 1 year time range in such way:
        365 days * 7 TG posts per day * 7 avg # of media in TG post (it parsed separately with `group_id` tag) ~= 17.500
        So i set 20.000 limit for posts.

        Returns number of posts scraped.
        """

        posts_scraped = 0
        batch_size: int = 200
        posts_batch = []

        logging.info(f"Starting to process posts from {from_date}")

        async for post in tg_client.iter_messages(self.channel, limit=limit):
            # Loop through posts from newest to `from_date` and stop when `limit` posts are scraped

            if post.date < from_date:
                break

            posts_batch.append(post)

            if len(posts_batch) >= batch_size:
                await self.scrape_posts_batch(posts_batch)
                posts_scraped += len(posts_batch)
                posts_batch = []
                logging.info(f"{posts_scraped} posts processed so far...")

        # Process any remaining messages
        if posts_batch:
            await self.scrape_posts_batch(posts_batch)
            posts_scraped += len(posts_batch)

        return posts_scraped
