from asyncio import get_event_loop
from datetime import datetime
from time import sleep
from typing import Optional, Literal

import pyrfc3339
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError
from pydantic import SecretStr, BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.clients.abstract_client import AbstractClient, UserEntry
from src.clients.clients_models import CollectConfig, ClientTaskConfig, BaseEnvSettings, ClientConfig
from src.const import ENV_FILE_PATH, PostType, CollectionStatus
from src.db import db_funcs
from src.db.db_funcs import submit_posts
from src.db.db_models import DBUser, DBPost
from src.misc.project_logging import get_b5_logger

logger = get_b5_logger(__file__)


class GoogleAPIKeySetting(BaseSettings):
    GOOGLE_API_KEYS: list[SecretStr]
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')


class YoutubeSearchParameters(BaseModel):
    q: Optional[str] = Field(alias="query")
    part: Optional[str] = Field("id")  # 'contentDetails,statistics,status,topicDetails,recordingDetails,localizations'
    type: Optional[str] = None
    location: Optional[str] = None
    locationRadius: Optional[str] = None
    maxResults: Optional[int] = Field(ge=0, le=50, default=5, alias="limit")
    order: Literal["date", "rating", "relevance", "title", "videoCount", "viewCount"] = "date"  # probably
    publishedAfter: Optional[str] = Field(alias="from_time")
    publishedBefore: Optional[str] = Field(alias="to_time")
    regionCode: Optional[str] = None  # ISO 3166-1 alpha-2


type TVYoutubeSearchParameters = YoutubeSearchParameters
type PostDict = dict
type UserDict = dict


class YoutubeClient[TVYoutubeSearchParameters, PostDict, UserDict](AbstractClient):

    def __init__(self, config: ClientConfig):
        super().__init__(config)
        self.client: Resource
        self.request_delay = 5
        self.api_key_index = -1
        self.has_keys_available = True
        self.client = None

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
            task: ClientTaskConfig = self._task_queue.pop()
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
        while task.has_more():
            task.update_current_config()
            # yt_config = self.transform_config(task.current_step_config)
            logger.debug(f"Getting data: {repr(task)}")
            result = get_event_loop().run_until_complete(self.collect(yt_config))
            if result is None:
                # raise ValueError("Could not fetch data")
                db_funcs.set_task_status(task.id, CollectionStatus.PAUSED)
                return False
            posts: list[DBPost] = [self.create_post_entry(post, task) for post in result]
            submit_posts(posts)
            task.next()
            sleep(self.request_delay)
        db_funcs.set_task_status(task.id, CollectionStatus.DONE)
        logger.info(f"{self.platform_name} task '{task.task_name}' finished")
        return True

    async def collect(self, config: YoutubeSearchParameters) -> list[dict]:

        video_ids = []
        # ,contentDetails,statistics,status,topicDetails,recordingDetails,localizations",
        part = getattr(config, "part")
        while self.has_keys_available:
            try:

                delattr(config, "part")
                config.part = "snippet"
                # region-code is automatically set to user locatin (e.g. ES)
                search_response = self.client.search().list(**config.model_dump()).execute()
                #video_ids = [item['id']['videoId'] for item in search_response.get('items', [])]
                return search_response.get('items', [])
            except HttpError as e:
                print(f"An HTTP error {e.resp.status} occurred:\n{e.content}")
                print(e.status_code)
                if e.status_code != 400:
                    self.set_new_client()

        """
        while self.has_keys_available:
            try:
                while True:
                    videos_response = self.client.videos().list(
                        part=part,
                        id=','.join(video_ids)
                    ).execute()

                    videos: list[dict] = []
                    for search_item, details_item in zip(search_response["items"], videos_response["items"]):
                        # print(search_item)
                        v = {
                                k: search_item[k] for k in ["id", "snippet"]
                            } | {
                                k: details_item.get(k) for k in
                                ["contentDetails", "status", "statistics", "topicDetails", "localizations"]
                                if k in details_item
                            }
                        videos.append(v)
                    return videos

            except HttpError as e:
                print(f"An HTTP error {e.resp.status} occurred:\n{e.content}")
                return None
        """

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
            collection_step=task.steps_done + 1
        )

    def create_user_entry(self, user: UserEntry) -> DBUser:
        pass

    @property
    def platform_name(self) -> str:
        return "youtube"
