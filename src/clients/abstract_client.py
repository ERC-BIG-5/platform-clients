from abc import ABC, abstractmethod
from datetime import datetime
from typing import TypeVar, Optional, TYPE_CHECKING

from pydantic import BaseModel

from databases.db_models import CollectionResult, DBPost, DBUser
from src.clients.clients_models import CollectConfig, ClientTaskConfig, ClientConfig
from tools.project_logging import get_logger

if TYPE_CHECKING:
    from src.platform_manager import PlatformManager

TClientConfig = TypeVar("TClientConfig", bound=BaseModel)
PostEntry = TypeVar("PostEntry")
UserEntry = TypeVar("UserEntry")


class AbstractClient[TClientConfig, PostEntry, UserEntry](ABC):

    def __init__(self, config: ClientConfig):
        self.config = config
        self._task_queue: list[ClientTaskConfig] = []
        self.manager: Optional["PlatformManager"]
        self.logger = get_logger(__name__)

    @abstractmethod
    def setup(self):
        """
        client specific setup function might be sync or async
        :return:
        """
        pass

    @abstractmethod
    def transform_config(self, abstract_config: CollectConfig) -> TClientConfig:
        """
        transform the generic configuration into a platform specific configuration
        :param abstract_config:
        :return:
        """
        pass

    async def execute_task(self, task: ClientTaskConfig) -> CollectionResult:
        start_time = datetime.now()
        collected_items = await self.collect(
            task.collection_config
        )

        posts: list[DBPost] = []
        users: set[DBUser] = set()
        # Process results
        for item in collected_items:
            posts.append(self.create_post_entry(item, task))
            users.add(self.create_user_entry(item))

        return CollectionResult(
            posts = posts,
            users = list(users),
            task=task,
            collected_items= len(collected_items),
            duration=int((datetime.now() - start_time).total_seconds() * 1000) # millis
        )

    @abstractmethod
    async def collect(self, collection_config: CollectConfig) -> list[PostEntry]:
        """
        Make a specific collection (step of a task). This function should use
        the client API
        :param collect_settings:
        :param generic_config:
        :return:
        """
        pass

    def default_post_data(self, task: ClientTaskConfig):
        """
        TODO Try this method. this is in order to reduce some boilerplate in
        the create_post_entry function.
        :param task:
        :return:
        """
        return {
            "platform": self.platform_name,
            "date_collected": datetime.now(),
            "collection_task_id": task.id,
            # "collection_step": task.steps_done + 1
        }

    @abstractmethod
    def create_post_entry(self, post: PostEntry, task: ClientTaskConfig) -> DBPost:
        pass

    @abstractmethod
    def create_user_entry(self, user: UserEntry) -> DBUser:
        pass

    @property
    @abstractmethod
    def platform_name(self) -> str:
        pass
