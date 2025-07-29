import logging
import time
from contextlib import aclosing
from datetime import datetime
from typing import Optional, Protocol

import orjson
from pydantic import Field, BaseModel, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from twscrape import API
from twscrape.api import API as TwitterAPI

from big5_databases.databases.db_models import DBPost, DBUser
from big5_databases.databases.external import PostType, CollectConfig, ClientTaskConfig, ClientConfig
from src.clients.abstract_client import AbstractClient
from src.const import ENV_FILE_PATH
from src.platform_manager import PlatformManager
from tools.pydantic_annotated_types import SerializableDatetimeAlways


class TwitterAuthSettings(BaseSettings):
    username: str = Field(alias="TWITTER_USERNAME")
    password: SecretStr = Field(alias="TWITTER_PASSWORD")
    email: str = Field(alias="TWITTER_EMAIL")
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')


class TwitterSearchParameters(BaseModel):
    query: Optional[str] = Field(default="")
    lang: str = "en"
    filter_replies: bool = True
    filter_quotes: bool = True
    from_time: Optional[SerializableDatetimeAlways] = None
    to_time: Optional[SerializableDatetimeAlways] = None
    limit: Optional[int] = 100
    geocode: Optional[str] = None

    def build_query(self) -> str:
        """Build the Twitter search query string"""
        q = f"{self.query} lang:{self.lang}"

        if self.filter_replies:
            q += " -filter:replies"
        if self.filter_quotes:
            q += " -filter:quote"

        if self.from_time:
            q += f" since:{self.from_time.strftime('%Y-%m-%d_%H:%M:%S_UTC')}"
        if self.to_time:
            q += f" until:{self.to_time.strftime('%Y-%m-%d_%H:%M:%S_UTC')}"
        if self.geocode:
            q += f" geocode:{self.geocode}"
        return q


class TwitterResource(Protocol):
    async def search(self, query: str): ...

    async def pool(self): ...


class TwitterClient(AbstractClient[TwitterSearchParameters, dict, dict]):
    """
    Twitter client implementation using twscrape library with integrated management
    """

    def __init__(self, config: ClientConfig, manager: PlatformManager):
        super().__init__(config, manager)
        self.api: Optional[TwitterAPI] = None
        self.settings: Optional[TwitterAuthSettings] = None
        # self.platform_db = platform_db

        # Rate limiting attributes
        self.rate_limit_window = 900  # 15 minutes in seconds
        self.rate_limit_requests = 180  # Requests per window
        self.request_timestamps: list[float] = []
        self._accounts_initialized = False

        self.logger = logging.getLogger(__file__)

    def setup(self):
        """Initialize the Twitter API client with authentication"""
        self.settings = TwitterAuthSettings()
        self.api = API()  # or API("path-to.db") for custom DB path

    async def _ensure_accounts_initialized(self):
        """Initialize authentication with Twitter"""
        if not self._accounts_initialized:
            await self.initialize_auth()
            self._accounts_initialized = True

    async def initialize_auth(self):
        """Initialize authentication with Twitter"""
        if not self.api:
            self.setup()

        accounts = await self.api.pool.get_all()

        client_user = None
        for acc in accounts:
            if acc.username == self.settings.username:
                client_user = acc
                break
        if not client_user:
            # Add account credentials
            await self.api.pool.add_account(
                self.settings.username,
                self.settings.password.get_secret_value(),
                self.settings.email,
                self.settings.password.get_secret_value()
            )
            client_user = await self.api.pool.get(self.settings.username)

        if not client_user.active:
            await self.api.pool.login(client_user)

    def _check_rate_limit(self):
        """Manage rate limiting for Twitter API"""
        current_time = time.time()

        # Remove timestamps older than the window
        self.request_timestamps = [ts for ts in self.request_timestamps
                                   if current_time - ts < self.rate_limit_window]

        # If we're at the limit, wait until we can make another request
        if len(self.request_timestamps) >= self.rate_limit_requests:
            sleep_time = self.request_timestamps[0] + self.rate_limit_window - current_time
            if sleep_time > 0:
                self.logger.info(f"Rate limit reached, waiting {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                self.request_timestamps.pop(0)

        # Add current request timestamp
        self.request_timestamps.append(current_time)

    @staticmethod
    def transform_config(abstract_config: CollectConfig) -> TwitterSearchParameters:
        """Transform generic config to Twitter-specific parameters"""
        model = TwitterSearchParameters.model_validate(abstract_config, from_attributes=True)
        return model

    @staticmethod
    def transform_config_to_serializable(abstract_config: CollectConfig) -> TwitterSearchParameters:
        return TwitterClient.transform_config(abstract_config)

    async def collect(self, generic_config: CollectConfig) -> list[dict]:
        """Collect tweets based on search parameters"""
        await self._ensure_accounts_initialized()

        if not self.api:
            await self.initialize_auth()

        self._check_rate_limit()

        config = self.transform_config(generic_config)
        tweets = []
        query = config.build_query()

        try:
            async with aclosing(self.api.search(query)) as gen:
                async for tweet in gen:
                    tweets.append(tweet.dict())
                    if len(tweets) >= config.limit:
                        break

            self.logger.info(f"Collected {len(tweets)} tweets for query: {query}")
            return tweets

        except Exception as e:
            self.logger.error(f"Error collecting tweets: {str(e)}")
            raise

    # async def process_task(self, task: ClientTaskConfig) -> list[DBPost]:
    #     """
    #     Execute Twitter collection task with specific handling for:
    #     - Rate limiting
    #     - Tweet metadata collection
    #     - User data collection
    #     """
    #     try:
    #         if self.platform_db:
    #             self.platform_db.update_task_status(task.id, CollectionStatus.RUNNING)
    #         start_time = datetime.now()
    #
    #         # Execute collection
    #         collected_items = await self.collect(task.collection_config)
    #
    #         # Process results and create entries
    #         posts: list[DBPost] = []
    #         users = set()  # Use set to avoid duplicate users
    #
    #         for item in collected_items:
    #             # Create post-entry (tweet)
    #             post = self.create_post_entry(item, task)
    #             posts.append(post)
    #
    #             # Create user entry
    #             if 'user_data' in item:
    #                 user = self.create_user_entry(item['user_data'])
    #                 users.add(user)
    #
    #         # Submit posts and users to database
    #         if self.platform_db:
    #             posts = self.platform_db.db_mgmt.safe_submit_posts(posts)
    #             duration = (datetime.now() - start_time).total_seconds()
    #             self.platform_db.db_mgmt.update_task(task.id,
    #                                                  CollectionStatus.DONE,
    #                                                  len(collected_items),
    #                                                  len(posts),
    #                                                  duration)
    #
    #         return posts
    #
    #     except Exception as e:
    #         self.logger.error(f"Error executing Twitter task {task.task_name}: {str(e)}")
    #         if self.platform_db:
    #             self.platform_db.update_task_status(task.id, CollectionStatus.ABORTED)
    #         raise e

    def create_post_entry(self, post: dict, task: ClientTaskConfig) -> DBPost:
        """Create a database post-entry from a tweet"""
        return DBPost(
            platform="twitter",
            platform_id=str(post['id']),
            post_url=f"https://x.com/{post['user']['username']}/status/{post['id']}",
            date_created=post["date"],
            date_collected=datetime.now(),
            post_type=PostType.REGULAR,
            content=orjson.loads(orjson.dumps(post)),
            collection_task_id=task.id
        )

    def create_user_entry(self, user: dict) -> DBUser:
        """Create a database user entry from a Twitter user"""
        return DBUser(
            platform="twitter",
            platform_username=user.get('username')
        )

    @property
    def platform_name(self) -> str:
        return "twitter"