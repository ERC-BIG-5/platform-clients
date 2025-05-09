from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import TypeVar, Optional, TYPE_CHECKING

from pydantic import BaseModel

from databases.db_models import CollectionResult, DBPost, DBUser
from databases.external import ClientTaskConfig, CollectConfig, ClientConfig
from tools.project_logging import get_logger

if TYPE_CHECKING:
    from src.platform_manager import PlatformManager

TClientConfig = TypeVar("TClientConfig", bound=BaseModel)
PostEntry = TypeVar("PostEntry")
UserEntry = TypeVar("UserEntry")


class CollectionException(Exception):
    orig_exception: Exception

    def __init__(self, orig_exception: Exception) -> None:
        self.orig_exception = orig_exception


class QuotaExceeded(CollectionException):

    def __init__(self, blocked_until: datetime, orig_exception: Exception) -> None:
        super().__init__(orig_exception)
        self.blocked_until = blocked_until

    @classmethod
    def next_day(self, orig_exception: Exception) -> "QuotaExceeded":
        tomorrow = datetime.now() + timedelta(days=1)
        block_time = datetime(tomorrow.year, tomorrow.month, tomorrow.day)
        return QuotaExceeded(blocked_until=block_time, orig_exception=orig_exception)

    @classmethod
    def twenty_four_hours(cls, orig_exception: Exception) -> "QuotaExceeded":
        tomorrow = datetime.now() + timedelta(hours=24)
        return QuotaExceeded(blocked_until=tomorrow, orig_exception=orig_exception)


class AbstractClient[TClientConfig, PostEntry, UserEntry](ABC):

    def __init__(self, config: ClientConfig, manager: "PlatformManager"):
        self.config = config
        self._task_queue: list[ClientTaskConfig] = []
        self.manager: Optional[PlatformManager] = manager
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

    async def execute_task(self, task: ClientTaskConfig) -> CollectionResult | CollectionException:
        start_time = datetime.now()
        try:
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
                posts=posts,
                added_posts=[],
                users=list(users),
                task=task,
                collected_items=len(collected_items),
                duration=int((datetime.now() - start_time).total_seconds() * 1000)  # millis
            )
        except CollectionException as e:
            return e

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
    def platform_name(self) -> str:
        return self.manager.platform_name

    def raw_post_data_conversion(self, data: dict) -> PostEntry:
        raise NotImplementedError("This method should be implemented in the client")
