import asyncio
import inspect
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TypeVar, Optional, TYPE_CHECKING

from pydantic import BaseModel

from src.clients.clients_models import CollectConfig, ClientTaskConfig, ClientConfig
from src.db.db_models import DBPost, DBUser

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

    def add_tasks(self, task_queue: list[ClientTaskConfig]):
        """
        Add a list of tasks to the client
        :param task_queue:
        :return:
        """
        self._task_queue.extend(task_queue)

    @abstractmethod
    def continue_tasks(self):
        """
        Continue all tasks in sequence
        :return:
        """
        pass

    @abstractmethod
    def continue_task(self, task: ClientTaskConfig) -> bool:
        """
        Continue a task
        :param task:
        :return:
        """
        pass

    @abstractmethod
    async def collect(self, collect_settings: CollectConfig, generic_config: CollectConfig) -> list[PostEntry]:
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
