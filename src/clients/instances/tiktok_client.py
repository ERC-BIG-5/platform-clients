from datetime import datetime, timezone
from typing import Optional, Literal, Any, TypedDict

from pydantic import SecretStr, Field, BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from tiktok_research_api import TikTokResearchAPI, Criteria, QueryVideoRequest, Query

from databases.db_models import DBUser, DBPost
from databases.external import ClientConfig
from src.clients.abstract_client import AbstractClient
from src.clients.clients_models import ClientTaskConfig, CollectConfig
from src.const import ENV_FILE_PATH
from tools.project_logging import get_logger


class TikTokPISetting(BaseSettings):
    TIKTOK_CLIENT_KEY: str
    TIKTOK_CLIENT_SECRET: SecretStr
    RATE_LIMIT: int = 5
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')


EU_COUNTRY_CODES = [
    'AT', 'BE', 'BG', 'CY', 'CZ', 'DE', 'DK', 'EE', 'ES', 'FI',
    'FR', 'GR', 'HR', 'HU', 'IE', 'IT', 'LT', 'LU', 'LV', 'MT',
    'NL', 'PL', 'PT', 'RO', 'SE', 'SI', 'SK'
]

PUBLIC_VIDEO_QUERY_FIELDS = {
    "id", "video_description", "create_time", "region_code", "share_count",
    "view_count", "like_count", "comment_count", "music_id", "hashtag_names",
    "username", "effect_ids", "playlist_id", "voice_to_text", "is_stem_verified",
    "video_duration", "hashtag_info_list", "video_mention_list", "video_label"
}

VideoFields = Literal[
    "create_date",  # when the video was created
    "username",  # the handle/username of the creator
    "region_code",  # the region where the video was uploaded
    "video_id",  # the unique ID of the video
    "hashtag_name",  # indexed hashtag
    "keyword",  # a string in the video description (can be a hashtag or something else)
    "music_id",  # the unique ID of the audio
    "effect_id",  # the unique ID of the effects used
    "video_length"  # the length of the video in seconds
]


class CriteriaModel(BaseModel):
    operation: Literal["EQ", "IN", "GT", "GTE", "LT", "LTE"]
    field_name: VideoFields
    field_values: list[Any]


class QueryModel(BaseModel):
    and_: Optional[list[CriteriaModel]] = Field(default_factory=list, alias="and")
    or_: Optional[list[CriteriaModel]] = Field(default_factory=list, alias="or")
    not_: Optional[list[CriteriaModel]] = Field(default_factory=list, alias="not")


class QueryVideoResult(BaseModel):
    id: Optional[int]
    video_description: Optional[str]
    create_time: Optional[int]  # assuming ISO format timestamp
    region_code: Optional[str]
    share_count: Optional[int]
    view_count: Optional[int]
    like_count: Optional[int]
    comment_count: Optional[int]
    music_id: Optional[int]
    hashtag_names: list[str]
    username: Optional[str]
    effect_ids: list[str]
    playlist_id: Optional[int]
    voice_to_text: Optional[Any]
    is_stem_verified: Optional[bool]
    video_duration: Optional[int]  # in seconds
    hashtag_info_list: list[TypedDict("hashtag_info_list", {"hashtag_id": int, "hashtag_name": str,
                                                            "hashtag_description": str})]  # you might want to create a separate HashtagInfo model
    video_mention_list: list[str]  # you might want to create a separate VideoMention model
    video_label: Optional[
        TypedDict("video_label", {"type": int, "vote": bool, "warn": bool, "content": str, "sink": bool})]

    @property
    def video_url(self):
        return f"https://www.tiktok.com/@{self.username}/video/{self.id}"


class UserProfile(BaseModel):
    username: str
    is_verified: bool
    likes_count: int
    video_count: int
    avatar_url: str
    bio_description: str
    display_name: str
    follower_count: int
    following_count: int


class TikTokClient(AbstractClient[QueryVideoRequest, QueryVideoResult, UserProfile]):

    def __init__(self, config: ClientConfig):
        super().__init__(config)
        self.client: Optional[TikTokResearchAPI] = None
        self.logger = get_logger(__name__)

    def setup(self):
        self.settings = TikTokPISetting()
        self.client = TikTokResearchAPI(self.settings.TIKTOK_CLIENT_KEY,
                                        self.settings.TIKTOK_CLIENT_SECRET.get_secret_value(),
                                        self.settings.RATE_LIMIT)

    def transform_config(self, abstract_config: CollectConfig) -> QueryVideoRequest:
        start_time = datetime.strptime(abstract_config.from_time)
        start_time_s = start_time.strftime("%Y%m%d")
        end_date = datetime.strptime(abstract_config.to_time)
        end_date_s = end_date.strftime("%Y%m%d")

        query = QueryModel.model_validate(abstract_config.query)
        query_dict = query.model_dump()

        if not abstract_config.fields:
            abstract_config.fields = PUBLIC_VIDEO_QUERY_FIELDS
        return QueryVideoRequest(start_date=start_time_s,
                                 query=query_dict,
                                 end_date=end_date_s,
                                 fields=",".join(abstract_config.fields),
                                 max_count=min(100, abstract_config.limit),
                                 max_total=abstract_config.limit)

    async def collect(self, collection_config: CollectConfig) -> list[dict]:
        config = self.transform_config(collection_config)

        all_videos = []
        while True:
            videos, search_id, cursor, has_more, start_date, end_date = self.client.query_videos(config,
                                                                                                 fetch_all_pages=True)
            all_videos.extend(videos)
            if len(all_videos) >= config.max_total:
                break

        return all_videos

    def create_post_entry(self, post: QueryVideoResult, task: ClientTaskConfig) -> DBPost:
        return DBPost(
            post_url=post.video_url,
            platform=self.platform_name,
            date_created=datetime.fromtimestamp(post.create_time, tz=timezone.utc),
            content=post.model_dump(),
            collection_task_id=task.id
        )

    def create_user_entry(self, user: UserProfile) -> DBUser:
        return DBUser()

    @property
    def platform_name(self) -> str:
        return "tiktok"
