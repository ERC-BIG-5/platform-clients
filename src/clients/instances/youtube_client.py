from asyncio import get_event_loop
from datetime import datetime
from typing import Optional, Literal

import pyrfc3339
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError
from pydantic import SecretStr, BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.const import ENV_FILE_PATH, PostType, CollectionStatus
from src.clients.abstract_client import AbstractClient, UserEntry, PostEntry
from src.clients.clients_models import CollectConfig, ClientTaskConfig
from src.db import db_funcs
from src.db.db_funcs import submit_posts
from src.db.db_models import DBUser, DBPost
from src.misc.project_logging import get_b5_logger

logger = get_b5_logger(__file__)


class GoogleAPIKeySetting(BaseSettings):
    GOOGLE_API_KEY: SecretStr
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')


class YoutubeSearchParameters(BaseModel):
    q: Optional[str] = Field(alias="query")
    part: str
    type: Optional[str] = None
    location: Optional[str] = None
    locationRadius: Optional[str] = None
    maxResults: Optional[int] = Field(ge=0, le=50, default=5, alias="limit")
    order: Literal["date", "rating", "relevance", "title", "videoCount", "viewCount"] = "date"  # probably
    publishedAfter: Optional[str] = Field(alias="from_time")
    publishedBefore: Optional[str] = Field(alias="to_time")
    regionCode: Optional[str] = None  # ISO 3166-1 alpha-2


type TVYoutubeSearchParameters = YoutubeSearchParameters


class YoutubeClient[TVYoutubeSearchParameters, dict](AbstractClient):

    def __init__(self, config):
        super().__init__(config)
        self.client: Resource

    def setup(self):
        if self.config:
            print(self.config)
        API_KEY = GoogleAPIKeySetting().GOOGLE_API_KEY.get_secret_value()
        self.client = build('youtube', 'v3', developerKey=API_KEY)

    def transform_config(self, abstract_config: CollectConfig) -> YoutubeSearchParameters:
        return YoutubeSearchParameters.model_validate(abstract_config, from_attributes=True)

    def continue_tasks(self):
        logger.info(f"{self.platform_name}, continue with task queue")
        while self._task_queue:
            task: ClientTaskConfig = self._task_queue.pop()
            finished = self.continue_task(task)
         # log when they dont all finish
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
            yt_config = self.transform_config(task.current_step_config)
            result = get_event_loop().run_until_complete(self.collect(yt_config))
            if result is None:
                # raise ValueError("Could not fetch data")
                db_funcs.set_task_status(task.id, CollectionStatus.PAUSED)
                return False
            posts: list[DBPost] = [self.create_post_entry(post, task) for post in result]
            submit_posts(posts)
            task.next()
        db_funcs.set_task_status(task.id, CollectionStatus.DONE)
        logger.info(f"{self.platform_name} task '{task.task_name}' finished")
        return True

    async def collect(self, config: YoutubeSearchParameters) -> list[dict]:
        try:
            search_response = self.client.search().list(
                **config.model_dump()).execute()

            video_ids = [item['id']['videoId'] for item in search_response.get('items', [])]

            videos_response = self.client.videos().list(
                part='contentDetails,statistics,status,topicDetails,recordingDetails,localizations',
                id=','.join(video_ids)
            ).execute()

            videos: list[dict] = []
            for search_item, details_item in zip(search_response["items"], videos_response["items"]):
                print(search_item)
                v = {
                        k: search_item[k] for k in ["id", "snippet"]
                    } | {
                        k: details_item.get(k) for k in
                        ["contentDetails","status", "statistics", "topicDetails", "localizations"]
                    }
                videos.append(v)
            return videos

        except HttpError as e:
            print(f"An HTTP error {e.resp.status} occurred:\n{e.content}")
            return None

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
