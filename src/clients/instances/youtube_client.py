from typing import Optional, Literal

from googleapiclient.discovery import build, Resource
from pydantic import SecretStr, BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.const import ENV_FILE_PATH
from src.clients.abstract_client import AbstractClient, UserEntry
from src.clients.clients_models import CollectConfig
from src.db.db_models import DBUser, DBPost


class GoogleAPIKeySetting(BaseSettings):
    GOOGLE_API_KEY: SecretStr
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')


class YoutubeSearchParameters(BaseModel):
    query: Optional[str]
    location: Optional[str] = None
    locationRadius: Optional[str] = None
    maxResults: Optional[int] = Field(ge=0, le=50, default=5)
    order: Literal["date", "rating", "relevance", "title", "videoCount", "viewCount"] = "date"  # probably
    publishedAfter: Optional[str] = None
    publishedBefore: Optional[str] = None
    regionCode: Optional[str] = None  # ISO 3166-1 alpha-2


class YoutubeClient[YoutubeSearchParameters](AbstractClient):

    def __init__(self, config: CollectConfig):
        super().__init__(config)
        self.client: Resource

    def setup(self):
        API_KEY = GoogleAPIKeySetting().GOOGLE_API_KEY.get_secret_value()
        self.client = build('youtube', 'v3', developerKey=API_KEY)

    def transform_config(self, abstract_config: CollectConfig) -> YoutubeSearchParameters:
        ac = abstract_config
        return YoutubeSearchParameters(
            query=ac.query,
            publishedAfter=ac.from_time,
            publishedBefore=ac.to_time
        )

    async def collect(self, collect_settings: CollectConfig) -> list[PostEntry]:
        pass

    def create_post_entry(self, post: PostEntry) -> DBPost:
        pass

    def create_user_entry(self, user: UserEntry) -> DBUser:
        pass

    @property
    def platform_name(self) -> str:
        return "youtube"
