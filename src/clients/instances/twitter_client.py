from datetime import datetime

from pydantic import SecretStr, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from twscrape import API, gather, User

from src.const import PLATFORM_TWITTER, MISC_PATH, PostType
from src.clients.abstract_client import AbstractClient, CollectConfig
from src.db.db_models import DBUser, DBPost
from src.misc.project_logging import get_b5_logger

logger = get_b5_logger(__file__)

# todo, this should not be a setting or not only, but also come from a json/yaml file
class TwitterConfig(BaseSettings):
    username: str = Field(alias="TWITTER_USERNAME")
    password: SecretStr = Field(alias="TWITTER_PASSWORD")
    email: str = Field(alias="TWITTER_EMAIL")
    email_password: SecretStr = Field(alias="TWITTER_PASSWORD")

    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='allow')


class TwitterClient[Tweet](AbstractClient):

    def __init__(self, config: TwitterConfig):
        super().__init__(config)
        self.config: TwitterConfig = config
        self.api: API

    async def setup(self):
        logger.debug("Twitter clients setup")
        self.api = API(
            (MISC_PATH / "twitter_api_accounts.sqlite").absolute().as_posix())  # or API("path-to.db") - default is `accounts.db`
        # ADD ACCOUNTS (for CLI usage see BELOW)
        # check if we have a user already, if not, add them
        # todo, check if the credentials differ from accounts added...
        if not await self.api.pool.get_all():
            await self.api.pool.add_account(
                self.config.username,
                self.config.password.get_secret_value(),
                self.config.email,
                self.config.email_password.get_secret_value())

        await self.api.pool.login_all()

    async def collect(self, collect_settings: CollectConfig) -> list[Tweet]:
        query_a = [collect_settings.query]
        if ft := collect_settings.from_time:
            query_a.append(f"since:{ft}")
        if tt := collect_settings.to_time:
            query_a.append(f"until:{tt}")
        if extra := collect_settings.extra:
            query_a.extend([f"{k}:{v}" for k, v in extra.items()])
        tweets: list[Tweet] = await gather(
            self.api.search(" ".join(query_a), limit=collect_settings.limit))
        for tweet in tweets:
            post_entry = self.create_post_entry(tweet)
            user = self.create_user_entry(tweet.tw_usr)
            post_entry.user = user
        return tweets

    def create_post_entry(self, post: Tweet) -> DBPost:
        return DBPost(
            platform=PLATFORM_TWITTER,
            post_url=post.url,
            date_created=post.date,
            date_collected=datetime.now(),
            post_type=PostType.REGULAR,
            content=post.json(),
            collection_method_id="test"
        )

    def create_user_entry(self, user: User) -> DBUser:
        return DBUser(
            platform=PLATFORM_TWITTER,
            platform_username=user.username
        )

    @property
    def platform_name(self) -> str:
        return "twitter"


if __name__ == "__main__":
    TwitterClient(TwitterConfig())
