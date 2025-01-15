import asyncio
from datetime import datetime
from pathlib import Path
from typing import Optional, Literal, Sequence, Union, Protocol

import itertools
import more_itertools
import pyrfc3339
import yt_dlp
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pydantic import SecretStr, BaseModel, Field, field_validator, field_serializer
from pydantic_settings import BaseSettings, SettingsConfigDict

from databases.db_models import DBPost, DBUser
from src.clients.abstract_client import AbstractClient, UserEntry
from src.clients.clients_models import CollectConfig, ClientTaskConfig, ClientConfig
from src.const import ENV_FILE_PATH, PostType, CLIENTS_DATA_PATH
from tools.project_logging import get_logger


class GoogleAPIKeySetting(BaseSettings):
    GOOGLE_API_KEYS: SecretStr
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')


YT_VID_URL_PRE = "https://www.youtube.com/watch?v="


class YoutubeSearchParameters(BaseModel):
    # Search parameters
    q: Optional[str] = Field(
        default=None,
        description="Query term to search for",
        alias="query"
    )
    # 'contentDetails,statistics,status,topicDetails,recordingDetails,localizations'
    # id,snippet
    part: Optional[str] = Field(
        default="id",
        description="Specifies response resource parts"
    )

    @field_validator("part", mode="before")
    def convert_part_list(cls, value: list[str]):
        return ",".join(value)

    # "channel","playlist","video"
    type: Optional[Literal["video", "channel", "playlist"]] = Field(
        default="video",  # [channel,playlist,video]
        description="Resource type to retrieve (video,channel,playlist)"
    )

    # Location parameters
    location: Optional[str] = Field(
        default=None,
        description="Geographic coordinates (latitude,longitude)"
    )

    locationRadius: Optional[str] = Field(
        default=None,
        description="Circular geographic area radius (e.g. '1500m', '5km', '10000ft', '0.75mi')"
    )

    # Result control parameters
    maxResults: Optional[int] = Field(
        ge=0,
        le=50,
        default=2,
        description="Maximum number of items to return"
    )

    order: Literal["date", "rating", "relevance", "title", "videoCount", "viewCount"] = Field(
        default="date",
        description="Method to order resources in the API response"
    )

    pageToken: Optional[str] = Field(
        default=None,
        description="Specific page token in the result set"
    )

    # Time parameters
    publishedAfter: Optional[datetime] = Field(
        default=None,
        alias="from_time",
        description="Only return resources created after this datetime"
    )

    publishedBefore: Optional[datetime] = Field(
        default=None,
        alias="to_time",
        description="Only return resources created before this datetime")

    # Region and language parameters
    regionCode: Optional[str] = Field(
        default=None,
        description="ISO 3166-1 alpha-2 country code"
    )

    relevanceLanguage: Optional[str] = Field(
        default=None,
        description="ISO 639-1 two-letter language code",
        alias="language"
    )

    # Channel parameters
    channelId: Optional[str] = Field(
        default=None,
        description="Only contain resources created by the channel"
    )

    channelType: Optional[Literal["any", "show"]] = Field(
        default=None,
        description="Type of channel to restrict search to"
    )

    # Event parameters
    eventType: Optional[Literal["completed", "live", "upcoming"]] = Field(
        default=None,
        description="Type of broadcast event"
    )

    # Content filtering parameters
    safeSearch: Optional[Literal["moderate", "none", "strict"]] = Field(
        default="moderate",
        description="Whether to include restricted content"
    )

    # Video-specific parameters
    videoCaption: Optional[Literal["any", "closedCaption", "none"]] = Field(
        default=None,
        description="Filter based on caption availability"
    )

    # todo investigate....
    # https://developers.google.com/youtube/v3/docs/videoCategories
    videoCategoryId: Optional[str] = Field(
        default=None,
        description="Filter based on video category"
    )

    topicId: Optional[str] = Field(
        default=None,
        description="Filter based on topic. see ( https://gist.github.com/stpe/2951130dfc8f1d0d1a2ad736bef3b703 ) "
    )

    videoDefinition: Optional[Literal["any", "high", "standard"]] = Field(
        default=None,
        description="Filter based on video definition (HD/SD)"
    )

    videoDimension: Optional[Literal["2d", "3d", "any"]] = Field(
        default=None,
        description="Filter based on video dimension"
    )

    videoDuration: Optional[Literal["any", "long", "medium", "short"]] = Field(
        default=None,
        description="Filter based on video duration"
    )

    videoEmbeddable: Optional[Literal["any", "true"]] = Field(
        default=None,
        description="Filter to only embeddable videos"
    )

    videoLicense: Optional[Literal["any", "creativeCommon", "youtube"]] = Field(
        default=None,
        description="Filter based on video license type"
    )

    videoPaidProductPlacement: Optional[Literal["any", "true"]] = Field(
        default=None,
        description="Filter based on paid promotion presence"
    )

    videoSyndicated: Optional[Literal["any", "true"]] = Field(
        default=None,
        description="Filter to only syndicated videos"
    )

    videoType: Optional[Literal["any", "episode", "movie"]] = Field(
        default=None,
        description="Filter based on video type"
    )

    class Config:
        populate_by_name = True

    @field_validator("location")
    def location_validator(cls, val: Union[str, Sequence[float]]) -> Optional[str]:
        if isinstance(val, str):
            val = val.strip()
            lat, lon = [float(e) for e in val.split(",")]
            assert -90 <= lat <= 90
            assert -180 <= lon <= 180
            return val
        else:
            if len(val) < 2:
                get_logger(__file__).warning("Youtube search location must be a string or a list of two floats")
                return None
            else:
                lat, lon = val
                assert -90 <= lat <= 90
                assert -180 <= lon <= 180
                return f"({lat},{lon})"

    @field_serializer('publishedBefore')
    def serialize_publishedBefore(self, dt: datetime, _info):
        if dt:
            return dt.isoformat()

    @field_serializer('publishedAfter')
    def serialize_publishedAfter(self, dt: datetime, _info):
        if dt:
            return dt.isoformat()


type TVYoutubeSearchParameters = YoutubeSearchParameters
type PostDict = dict
type UserDict = dict


class YoutubePathConfig(BaseModel):
    pn: Path
    mp3s: Path = Field("mp3s", description="Where downloaded mp3s go")

    """
    @model_validator(mode="after")
    def validate_paths(self, paths: dict[str, Path]):
        print(paths)
        return paths
    """

    def get_path(self, v: str) -> Path:
        return getattr(self, v)


class YoutubeResource(Protocol):
    def search(self):
        pass

    def videos(self):
        pass


class YoutubeClient[TVYoutubeSearchParameters, PostDict, UserDict](AbstractClient):
    ALL_PUBLIC_PART_OPTIONS = ["contentDetails", "liveStreamingDetails",
                               "paidProductPlacementDetails", "player", "status", "statistics", "topicDetails",
                               "localizations"]
    ALL_PART_OPTIONS = ["id", "contentDetails", "liveStreamingDetails", "processingDetails", "snippet",
                        "paidProductPlacementDetails", "player", "fileDetails", "status", "statistics", "topicDetails",
                        "recordingDetails", "suggestions", "localizations"]
    DEFAULT_PART_OPTIONS = ["contentDetails", "status", "statistics", "topicDetails", "recordingDetails", "suggestions",
                            "localizations"]

    def __init__(self, config: ClientConfig):
        super().__init__(config)
        self.client: YoutubeResource = None
        self.request_delay = 0
        self.has_keys_available = True
        self.logger = get_logger(__name__)
        # todo refactor this into a superclass or interface
        self.path_config = YoutubePathConfig(pn=CLIENTS_DATA_PATH / self.platform_name)
        pass

    def setup(self):
        # if self.config and self.config.auth_config:
        #     ## todo generalize this and move it somewhere else
        #     env = BaseEnvSettings()
        #     self.settings = GoogleAPIKeySetting.model_validate({k: env.model_extra[v]
        #                                                         for k, v in self.config.auth_config.items()})
        # else:
        self.settings = GoogleAPIKeySetting()

        self.client = build('youtube', 'v3', developerKey=self.settings.GOOGLE_API_KEYS.get_secret_value())

        if self.config.request_delay:
            self.request_delay = self.config.request_delay

    def transform_config(self, abstract_config: CollectConfig) -> YoutubeSearchParameters:
        return YoutubeSearchParameters.model_validate(abstract_config, from_attributes=True)

    async def collect(self, generic_config: CollectConfig) -> list[dict]:

        # ,contentDetails,statistics,status,topicDetails,recordingDetails,localizations",
        config = self.transform_config(generic_config)
        part = getattr(config, "part")
        search_result_items = []
        pages = 0

        # rename in config to limit. we are always using 50 or lower, depending if there is a limit
        delattr(config, "part")
        config.part = "id,snippet"
        # todo, this needs testing!
        if "snippet" in part:
            parts = part.split(",")
            parts.remove("snippet")
            part = ",".join(parts)

        # has_more_pages = True
        while True:
            try:
                # region-code is automatically set to user locatin (e.g. ES)
                config.maxResults = min(50, generic_config.limit - len(search_result_items))  # remaining
                self.logger.debug(config.model_dump_json(exclude_none=True))
                search_response = self.client.search().list(**config.model_dump(exclude_none=True)).execute()
                pages += 1
                search_result_items.extend(search_response.get('items', []))
                if nextPageToken := search_response.get("nextPageToken"):
                    config.pageToken = nextPageToken
                else:
                    break
                if len(search_result_items) >= generic_config.limit:
                    break
            except HttpError as e:
                print(f"An HTTP error {e.resp.status} occurred:\n{e.content.decode('utf-8')}")
                break

        search_result_items = list(more_itertools.unique_everseen(search_result_items, key=lambda i: i["id"]["videoId"]))
        self.logger.info(f"# uniuue response items: {len(search_result_items)}; num pages: {pages}")
        video_ids = [_["id"]["videoId"] for _ in search_result_items]


        all_videos_results = []
        for batch in itertools.batched(video_ids, 50):
            try:
                videos_response = self.client.videos().list(
                    part=part,
                    id=','.join(batch)
                ).execute()
            except HttpError as err:
                self.logger.error(f"An HTTP error {err.resp.status} occurred:\n{err.content.decode('utf-8')}")
                all_videos_results.extend([{} for i in batch])
                continue
            all_videos_results.extend(videos_response.get('items', []))

        videos: list[dict] = []

        # match search and list responses, if they dont match...
        zipped: list[tuple[dict, dict]] = []
        if len(search_result_items) != len(all_videos_results):
            self.logger.warning(
                f"Number of videos returned ({len(search_result_items)}) does not match number of items ({len(all_videos_results)})"
            )
            response_items_map = {si["id"]["videoId"]: si for si in search_result_items}
            detail_items_map = {si["id"]: si for si in all_videos_results}
            for k, v in response_items_map.items():
                zipped.append((v, detail_items_map.get(k, {})))
        else:
            zipped = list(zip(search_result_items, all_videos_results))

        for search_item, details_item in zipped:
            v = {
                    k: search_item.get(k) for k in ["id", "snippet"] if k in search_item
                } | {
                    k: v for k, v in
                    details_item.items()
                    if isinstance(v, dict)
                }
            videos.append(v)

        sorted(videos, key=lambda i: i.get("snippet")["publishedAt"])
        self.logger.info(f"Collected {len(videos)} videos.")
        return videos

    def collect_sync(self, generic_config: CollectConfig) -> list[dict]:
        return asyncio.run(self.collect(generic_config))

    def create_post_entry(self, post: dict, task: ClientTaskConfig) -> DBPost:
        return DBPost(
            platform="youtube",
            platform_id=post['id']['videoId'],
            post_url=f"https://www.youtube.com/v/{post['id']['videoId']}",
            date_created=pyrfc3339.parse(post["snippet"]["publishedAt"]),
            date_collected=datetime.now(),
            post_type=PostType.REGULAR,
            content=post,
            collection_task_id=task.id,
        )

    def create_user_entry(self, user: UserEntry) -> DBUser:
        pass

    @property
    def platform_name(self) -> str:
        return "youtube"

    from pathlib import Path

    # Function to download and convert a YouTube video to MP3 format using yt-dlp
    def download_video_as_mp3(self, video_id) -> Path | None:
        video_url = f"{YT_VID_URL_PRE}{video_id}"
        dest_path = self.path_config.mp3s / f"{video_id}.mp3"
        if dest_path.exists():
            print(f"The file {dest_path} already exists. Skipping download...")
            return dest_path  # If already exists, don't download again

        try:
            ydl_opts = {
                'format': 'bestaudio/best',  # Download the best audio quality available
                'outtmpl': dest_path.absolute().as_posix().rstrip(".mp3"),  # Save the file with a sanitized name
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',  # Convert the audio to MP3 format
                    'preferredquality': '192',  # Set the MP3 quality
                }],
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                print(f"Downloading video from URL: {video_url}")
                ydl.download([video_url])  # Download the video and convert to MP3
            if dest_path.exists():
                print(f"Download and conversion successful: {dest_path}")
                return dest_path  # Return the file path if it exists
            else:
                print(f"Error: The file {dest_path} was not found after download.")
                return None
        except Exception as e:
            print(f"Error downloading {video_url}: {e}")
            return None  # Return None if an error occurs

    # print()
    # audio_file = os.path.join(output_path, sanitized_title + '.mp3')
