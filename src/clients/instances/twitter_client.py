import logging
from contextlib import aclosing
from datetime import datetime
from typing import Optional, Protocol, TYPE_CHECKING

import orjson
from pydantic import Field
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from twscrape import API
from twscrape.api import API as TwitterAPI

from databases.db_models import DBPost, DBUser
from databases.external import PostType, CollectConfig, ClientTaskConfig, ClientConfig
from src.clients.abstract_client import AbstractClient, UserEntry
from src.clients.clients_models import BaseEnvSettings
from src.const import ENV_FILE_PATH

if TYPE_CHECKING:
    from src.platform_mgmt.twitter_manager import TwitterManager


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
    to_time: Optional[datetime] = None
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
        if self.to_time:
            q += f" until:{self.to_time.strftime('%Y-%m-%d_%H:%M:%S_UTC')}"

        return q


class TwitterResource(Protocol):
    async def search(self, query: str): ...

    async def pool(self): ...


class TwitterClient(AbstractClient[TwitterSearchParameters, dict, dict]):
    """
    Updated Twitter client implementation using twscrape library
    """

    def __init__(self, config: ClientConfig, manager: "TwitterManager"):
        super().__init__(config, manager)
        self.api: Optional[TwitterAPI] = None
        self.settings: Optional[TwitterAuthSettings] = None
        self.logger = logging.getLogger(__file__)

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

        # Login all accounts in the pool
        # todo, bring back?
        # await self.api.pool.login_all()

    def transform_config(self, abstract_config: CollectConfig) -> TwitterSearchParameters:
        """Transform generic config to Twitter-specific parameters"""
        return TwitterSearchParameters.model_validate(abstract_config, from_attributes=True)

    async def collect(self, generic_config: CollectConfig) -> list[dict]:
        """Collect tweets based on search parameters"""

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
            # todo, check here if we are not authenticated anymore.
            # manager should be notified, so it can trigger trying to login again (even tho, the client is doing it)

            return tweets

        except Exception as e:
            self.logger.error(f"Error collecting tweets: {str(e)}")
            raise

    def create_post_entry(self, post: dict, task: ClientTaskConfig) -> DBPost:
        """Create a database post-entry from a tweet"""
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
