from datetime import datetime, timezone
from json import JSONDecodeError
from typing import Optional, Literal, Any, TypedDict, TYPE_CHECKING

from pydantic import SecretStr, Field, BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict
from tiktok_research_api import TikTokResearchAPI, Criteria, QueryVideoRequest, Query

from databases.db_models import DBUser, DBPost
from databases.external import ClientConfig, ClientTaskConfig, CollectConfig
from src.clients.abstract_client import AbstractClient, CollectionException
from src.const import ENV_FILE_PATH
from tools.project_logging import get_logger

if TYPE_CHECKING:
    from src.platform_mgmt.tiktok_manager import TikTokManager


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

    def to_criteria(self) -> Criteria:
        return Criteria(
            field_name=self.field_name,
            operation=self.operation,
            field_values=self.field_values
        )


class QueryModel(BaseModel):
    and_: Optional[list[CriteriaModel]] = Field(default_factory=list, alias="and")
    or_: Optional[list[CriteriaModel]] = Field(default_factory=list, alias="or")
    not_: Optional[list[CriteriaModel]] = Field(default_factory=list, alias="not")

    def to_query(self) -> Query:
        if not self.and_ and not self.or_ and not self.not_:
            self.and_.append(CriteriaModel(operation="IN",field_values=EU_COUNTRY_CODES,field_name="region_code"))
        return Query(
            and_criteria=[criteria.to_criteria() for criteria in self.and_],
            or_criteria=[criteria.to_criteria() for criteria in self.or_],
            not_criteria=[criteria.to_criteria() for criteria in self.not_]
        )


class QueryVideoResult(BaseModel):
    id: int
    video_description: Optional[str] = None
    create_time: Optional[int] = None  # assuming ISO format timestamp
    region_code: Optional[str] = None
    share_count: Optional[int] = None
    view_count: Optional[int] = None
    like_count: Optional[int] = None
    comment_count: Optional[int] = None
    music_id: Optional[int] = None
    hashtag_names: list[str] = None
    username: Optional[str] = None
    effect_ids: Optional[list[str]] = None
    playlist_id: Optional[int] = None
    voice_to_text: Optional[Any] = None
    is_stem_verified: Optional[bool] = None
    video_duration: Optional[int]  # in seconds
    hashtag_info_list: Optional[list[TypedDict("hashtag_info_list", {"hashtag_id": int, "hashtag_name": str,
                                                                     "hashtag_description": str})]] = None  # you might want to create a separate HashtagInfo model
    video_mention_list: Optional[list[str]] = None  # you might want to create a separate VideoMention model
    video_label: Optional[
        TypedDict("video_label", {"type": int, "vote": bool, "warn": bool, "content": str, "sink": bool})] = None

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

    def __init__(self, config: ClientConfig, manager: "TikTokManager"):
        super().__init__(config, manager)
        self.client: Optional[TikTokResearchAPI] = None
        self.logger = get_logger(__file__)

    def setup(self):
        self.settings = TikTokPISetting()
        self.client = TikTokResearchAPI(self.settings.TIKTOK_CLIENT_KEY,
                                        self.settings.TIKTOK_CLIENT_SECRET.get_secret_value(),
                                        self.settings.RATE_LIMIT)

    def transform_config(self, abstract_config: CollectConfig) -> QueryVideoRequest:
        if abstract_config.from_time:
            start_time = datetime.fromisoformat(abstract_config.from_time).date()
            start_time_s = start_time.strftime("%Y%m%d")
        if abstract_config.to_time:
            end_date = datetime.fromisoformat(abstract_config.to_time).date()
            end_date_s = end_date.strftime("%Y%m%d")

        # keyword <- abstract_config.query
        if not abstract_config.query:
            abstract_config.query = {"and_":[{
                "field_name": "region_code", "field_values": EU_COUNTRY_CODES,"operation":"in"
            }]}

        query = QueryModel.model_validate(abstract_config.query)

        if not hasattr(abstract_config, "fields"):
            abstract_config.fields = PUBLIC_VIDEO_QUERY_FIELDS
        return QueryVideoRequest(start_date=start_time_s,
                                 query=query.to_query(),
                                 end_date=end_date_s,
                                 fields=",".join(abstract_config.fields),
                                 max_count=min(100, abstract_config.limit),
                                 max_total=abstract_config.limit)

    async def collect(self, collection_config: CollectConfig) -> list[QueryVideoResult]:
        config = self.transform_config(collection_config)

        all_videos = []
        while True:
            try:
                videos, search_id, cursor, has_more, start_date, end_date = self.client.query_videos(config,
                                                                                                     fetch_all_pages=True)
            except JSONDecodeError as exc:
                print(exc)
                # todo, stop? mark abort
                return []
            except Exception as exc:
                print(exc)
                raise CollectionException(orig_exception=exc)
            all_videos.extend([
                self.raw_post_data_conversion(v) for v in videos])
            if len(all_videos) >= config.max_total:
                break

        return all_videos

    def create_post_entry(self, post: QueryVideoResult, task: ClientTaskConfig) -> DBPost:
        return DBPost(
            platform_id=str(post.id),
            post_url=post.video_url,
            platform=self.platform_name,
            date_created=datetime.fromtimestamp(post.create_time, tz=timezone.utc),
            content=post.model_dump(),
            collection_task_id=task.id
        )

    def create_user_entry(self, user: UserProfile) -> DBUser:
        return DBUser()

    def raw_post_data_conversion(self, post_data: dict) -> QueryVideoResult:
        return QueryVideoResult.model_validate(post_data)
