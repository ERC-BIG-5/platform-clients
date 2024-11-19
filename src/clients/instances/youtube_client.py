import itertools
from asyncio import get_event_loop
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Optional, Literal, Sequence, Union, Protocol

import pyrfc3339
import yt_dlp
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pydantic import SecretStr, BaseModel, Field, field_validator, field_serializer
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.clients.abstract_client import AbstractClient, UserEntry
from src.clients.clients_models import CollectConfig, ClientTaskConfig, BaseEnvSettings, ClientConfig
from src.const import ENV_FILE_PATH, PostType, CollectionStatus, CLIENTS_DATA_PATH
from src.db import db_funcs
from src.db.db_funcs import submit_posts
from src.db.db_models import DBUser, DBPost
from src.misc.project_logging import get_b5_logger

logger = get_b5_logger(__file__)


class GoogleAPIKeySetting(BaseSettings):
    GOOGLE_API_KEYS: list[SecretStr]
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
    type: Optional[str] = Field(
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
        default=50,
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
        description="Only return resources created before this datetime"
    )

    # Region and language parameters
    regionCode: Optional[str] = Field(
        default=None,
        description="ISO 3166-1 alpha-2 country code"
    )

    relevanceLanguage: Optional[str] = Field(
        default=None,
        description="ISO 639-1 two-letter language code"
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

    # todo investiate....
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
                logger.warning("Youtube search location must be a string or a list of two floats")
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
        self.api_key_index = -1
        self.has_keys_available = True
        # todo refactor this into a superclass or interface
        self.path_config = YoutubePathConfig(pn=CLIENTS_DATA_PATH / self.platform_name)
        pass

    def set_new_client(self) -> bool:
        """
        create a new client. returns False if there are no more keys available...
        """
        if self.api_key_index == len(self.settings.GOOGLE_API_KEYS) - 1:
            self.has_keys_available = False
            return False
        self.api_key_index = self.api_key_index + 1
        API_KEY = self.settings.GOOGLE_API_KEYS[self.api_key_index].get_secret_value()
        self.client = build('youtube', 'v3', developerKey=API_KEY)
        return True

    def setup(self):
        if self.config and self.config.auth_config:
            ## todo generalize this and move it somewhere else
            env = BaseEnvSettings()
            self.settings = GoogleAPIKeySetting.model_validate({k: env.model_extra[v]
                                                                for k, v in self.config.auth_config.items()})
        else:
            self.settings = GoogleAPIKeySetting()

        self.set_new_client()

        if self.config.request_delay:
            self.request_delay = self.config.request_delay

    def transform_config(self, abstract_config: CollectConfig) -> YoutubeSearchParameters:
        return YoutubeSearchParameters.model_validate(abstract_config, from_attributes=True)

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
        result = get_event_loop().run_until_complete(self.collect(yt_config))
        if result is None:
            # raise ValueError("Could not fetch data")
            db_funcs.set_task_status(task.id, CollectionStatus.PAUSED)
            return False
        posts: list[DBPost] = [self.create_post_entry(post, task) for post in result]
        submit_posts(posts)
        task.done()
        sleep(self.request_delay)
        db_funcs.set_task_status(task.id, CollectionStatus.DONE)
        logger.info(f"{self.platform_name} task '{task.task_name}' finished")
        return True

    async def collect(self, config: YoutubeSearchParameters) -> list[dict]:

        # ,contentDetails,statistics,status,topicDetails,recordingDetails,localizations",
        part = getattr(config, "part")
        all_response_items = []
        pages = 0
        # while self.has_keys_available:
        has_morePages = True
        # rename in config to limit. we are always using 50 or lower, depending if there is a limit
        limit = config.maxResults = 2  # TODO!!!
        delattr(config, "part")
        config.part = "id,snippet"

        while has_morePages:
            try:
                # region-code is automatically set to user locatin (e.g. ES)
                config.maxResults = min(50, limit - len(all_response_items))  # remaining
                search_response = self.client.search().list(**config.model_dump(exclude_none=True)).execute()
                pages += 1
                all_response_items.extend(search_response.get('items', []))
                if (nextPageToken := search_response.get("nextPageToken")):
                    config.pageToken = nextPageToken
                else:
                    has_morePages = False
                if len(all_response_items) >= limit:
                    break
            except HttpError as e:
                print(f"An HTTP error {e.resp.status} occurred:\n{e.content.decode('utf-8')}")
                has_morePages = False
                # remove those things...
                # if e.status_code != 400:
                #     self.set_new_client()
        logger.info(f"all responses: {len(all_response_items)}; num pages: {pages}")

        video_ids = [r["id"]["videoId"] for r in all_response_items]
        all_videos_results = []
        for batch in itertools.batched(video_ids, 50):
            try:
                videos_response = self.client.videos().list(
                    part=part,
                    id=','.join(batch)
                ).execute()
            except HttpError as err:
                logger.error(f"An HTTP error {err.resp.status} occurred:\n{err.content.decode('utf-8')}")
                all_videos_results.extend([{} for i in batch])
                continue
            all_videos_results.extend(videos_response.get('items', []))

        videos: list[dict] = []
        for search_item, details_item in zip(all_response_items, all_videos_results):
            # print(search_item)
            assert search_item["id"]["videoId"] == details_item["id"]
            v = {
                    k: search_item[k] for k in ["id", "snippet"]
                } | {
                    k: v for k, v in
                    details_item.items()
                    if isinstance(v, dict)
                }
            videos.append(v)

        # all_response_items_sorted = sorted(all_response_items, key=lambda i: i["snippet"]["publishedAt"])

        return videos

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
            # collection_step=task.steps_done + 1
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
