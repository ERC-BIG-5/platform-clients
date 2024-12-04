import json
from asyncio import get_event_loop

import orjson
import time
from datetime import datetime
from typing import Optional, Protocol
from contextlib import aclosing

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from twscrape import API
from twscrape.api import API as TwitterAPI

from src.clients.abstract_client import AbstractClient, UserEntry
from src.clients.clients_models import CollectConfig, ClientTaskConfig, BaseEnvSettings, ClientConfig
from src.const import ENV_FILE_PATH, PostType, CollectionStatus
from src.db import db_funcs
from src.db.db_models import DBPost, DBUser
from src.misc.project_logging import get_b5_logger
from pydantic import Field
logger = get_b5_logger(__file__)


class TwitterAuthSettings(BaseSettings):
    username: str = Field(alias="TWITTER_USERNAME")
    password: SecretStr = Field(alias="TWITTER_PASSWORD")
    email: str = Field(alias="TWITTER_EMAIL")
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')


class TwitterSearchParameters(BaseSettings):
    query: str
    lang: str = "en"
    filter_replies: bool = True
    filter_quotes: bool = True
    from_time: Optional[datetime] = None
    until_time: Optional[datetime] = None
    limit: int = 100

    def build_query(self) -> str:
        """Build the Twitter search query string"""
        q = f"{self.query} lang:{self.lang}"

        if self.filter_replies:
            q += " -filter:replies"
        if self.filter_quotes:
            q += " -filter:quote"

        if self.from_time:
            q += f" since:{self.from_time.strftime('%Y-%m-%d_%H:%M:%S_UTC')}"
        if self.until_time:
            q += f" until:{self.until_time.strftime('%Y-%m-%d_%H:%M:%S_UTC')}"

        return q


class TwitterResource(Protocol):
    async def search(self, query: str): ...

    async def pool(self): ...


class TwitterClient(AbstractClient):
    """
    Updated Twitter client implementation using twscrape library
    """

    def continue_tasks(self):
        logger.info(f"{self.platform_name}, continue with task queue")
        while self._task_queue:
            task: ClientTaskConfig = self._task_queue.pop(0)
            finished = self.continue_task(task)
        # log when they don't all finish
        logger.info(f"{self.platform_name} all tasks finished")

    def continue_task(self, task: ClientTaskConfig) -> bool:
        """
        :param task:
        :return: returns true we finished
        """
        task.status = CollectionStatus.RUNNING
        logger.info(f"continue task: {task.task_name}")
        # while task.has_more():
        # task.update_current_config()
        yt_config = self.transform_config(task.collection_config)
        logger.debug(f"Getting data: {repr(task)}")
        start_time = time.time()
        # todo a more specific type
        result: list[dict] = get_event_loop().run_until_complete(self.collect(yt_config, task.collection_config))
        duration = time.time() - start_time
        # todo do we ever get a None still?
        if result is None:
            # raise ValueError("Could not fetch data")
            db_funcs.set_task_status(task.id, CollectionStatus.PAUSED)
            return False

        return True

    def __init__(self, config: ClientConfig):
        super().__init__(config)
        self.api: Optional[TwitterAPI] = None
        self.settings: Optional[TwitterAuthSettings] = None

    def setup(self):
        """Initialize the Twitter API client with authentication"""
        if self.config and self.config.auth_config:
            env = BaseEnvSettings()
            self.settings = TwitterAuthSettings.model_validate({
                k: env.model_extra[v] for k, v in self.config.auth_config.items()
            })
        else:
            self.settings = TwitterAuthSettings()

        self.api = API()  # or API("path-to.db") for custom DB path

    async def initialize_auth(self):
        """Initialize authentication with Twitter"""
        if not self.api:
            self.setup()

        accounts = await self.api.pool.get_all()

        if any(acc.username == self.settings.username for acc in accounts):
            logger.debug(f"Account {self.settings.username} already exists")
            return

        # Add account credentials
        await self.api.pool.add_account(
            self.settings.username,
            self.settings.password.get_secret_value(),
            self.settings.email,
            self.settings.password.get_secret_value()
        )
        # Login all accounts in the pool
        await self.api.pool.login_all()

    def transform_config(self, abstract_config: CollectConfig) -> TwitterSearchParameters:
        """Transform generic config to Twitter-specific parameters"""
        return TwitterSearchParameters.model_validate(abstract_config, from_attributes = True)

    async def collect(self, config: TwitterSearchParameters, generic_config: CollectConfig) -> list[dict]:
        """Collect tweets based on search parameters"""
        if not self.api:
            await self.initialize_auth()

        tweets = []
        query = config.build_query()

        try:
            async with aclosing(self.api.search(query)) as gen:
                async for tweet in gen:
                    tweets.append(tweet.dict())
                    if len(tweets) >= config.limit:
                        break

            logger.info(f"Collected {len(tweets)} tweets for query: {query}")
            return tweets

        except Exception as e:
            logger.error(f"Error collecting tweets: {str(e)}")
            raise

    def create_post_entry(self, post: dict, task: ClientTaskConfig) -> DBPost:
        """Create a database post entry from a tweet"""
        # todo outsource the sanitazion hack with orjson and outsource
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

    def create_user_entry(self, user: UserEntry) -> DBUser:
        """Create a database user entry from a Twitter user"""
        return DBUser(
            platform="twitter",
            platform_username=user.get('username')
        )

    @property
    def platform_name(self) -> str:
        return "twitter"