import enum
from abc import abstractmethod
from asyncio import sleep, CancelledError
from datetime import datetime
from random import randint
from typing import TypeVar, Optional

import httpx

from big5_databases.databases.db_models import CollectionResult
from big5_databases.databases.external import CollectionStatus, ClientTaskConfig, ClientConfig
from big5_databases.databases.platform_db_mgmt import PlatformDB
from src.clients.abstract_client import AbstractClient, PostEntry, CollectionException, \
    QuotaExceeded
from src.const import BIG5_CONFIG
from src.misc.platform_quotas import store_quota, remove_quota, load_quotas
from tools.project_logging import get_logger

T_Client = TypeVar('T_Client', bound=AbstractClient)

class PlatformStatus(enum.Enum):
    idle = enum.auto()
    running = enum.auto()

class PlatformManager:
    """
    Base class for managing platform-specific operations including:
    - Client management
    - Database operations
    - Task queue management

    """

    def __init__(self, platform_name, client_class, client_config: ClientConfig):
        self.platform_name = platform_name
        self.client: AbstractClient = client_class(client_config, self)
        self.active : bool = True # can be set, with "progress" parameter in platform-config

        # Initialize platform database
        client_config.db_config.test_mode = BIG5_CONFIG.test_mode
        self.platform_db = PlatformDB(self.platform_name, client_config.db_config)
        # todo: test if this is needed
        self.client.manager = self
        self._active_tasks: list[ClientTaskConfig] = []
        self._client_setup = False
        self.logger = get_logger(__name__)
        self.current_quota_halt: Optional[datetime] = None
        self.status: PlatformStatus = PlatformStatus.idle

    @abstractmethod
    def _create_client(self, config: ClientConfig) -> T_Client:
        """Create platform-specific client instance"""
        return self.client

    def _setup_client(self):
        """Set up the client if not already set up"""
        if not self._client_setup and self.client:
            # logger.info(f"Setting up client for platform: {self.platform_name}")
            try:
                self.client.setup()
                self._client_setup = True
            except Exception as e:
                print(e)

    def add_tasks(self, tasks: list[ClientTaskConfig]) -> list[str]:
        """
        Adds the serializable collection_config of the specific platform
        Args:
            tasks:

        Returns:

        """
        for task in tasks:
            task.platform_collection_config = self.client.transform_config_to_serializable(task.collection_config)
        return self.platform_db.add_db_collection_tasks(tasks)

    def has_quota_halt(self) -> Optional[datetime]:
        """
        @returns: datetime if there is a halt, else None
        """
        if self.current_quota_halt:
            if datetime.now() < self.current_quota_halt:
                return self.current_quota_halt
            else:  # remove quota halt
                self.current_quota_halt = None
                remove_quota(self.platform_name)

    async def send_result(self, result: CollectionResult):
        try:
            host, port, path = BIG5_CONFIG.send_post_host, BIG5_CONFIG.send_post_port, BIG5_CONFIG.send_post_path
            httpx.post(f"{host}:{port}/{path}", json=[p.model_dump() for p in result.added_posts])
        except Exception as e:
            self.logger.error(e)
        except httpx.HTTPError as e:
            self.logger.warning(f"send_results failed: {e}")

    async def process_all_tasks(self) -> list[str]:
        """Process all pending tasks"""
        self._setup_client()
        processed_tasks = []
        self.status = PlatformStatus.running
        self.current_quota_halt = load_quotas().get(self.platform_name)
        if halt_until := self.has_quota_halt():
            self.logger.info(
                f"Progress for platform: '{self.platform_name}' deactivated due to quota halt, {halt_until:%Y.%m.%d - %H:%M}")
            return processed_tasks
        tasks = self.platform_db.get_pending_tasks()
        self.logger.debug(f"Task queue [{self.platform_name}]: {len(tasks)}")
        if not tasks:
            return processed_tasks
        for idx, task in enumerate(tasks):
            if (halt_until := self.has_quota_halt()):
                print(f"quota halt. not continuing tasks {halt_until:%Y.%m.%d - %H:%M}")
                return processed_tasks
            self.logger.debug(f"Processing task- platform:{task.platform}, id:{task.id}, {idx + 1}/{len(tasks)}")
            collection_result = await self.process_task(task)
            processed_tasks.append(collection_result)
            if BIG5_CONFIG.send_posts and isinstance(collection_result, CollectionResult):
                await self.send_result(collection_result)

            if idx == len(tasks) - 1:
                sleep_time = self.client.config.request_delay
                sleep_time += randint(0, self.client.config.delay_randomize)
                try:
                    await sleep(sleep_time)
                except (KeyboardInterrupt, CancelledError):
                    print("closing...")
                    return processed_tasks
        self.status = PlatformStatus.idle
        return processed_tasks

    async def process_task(self, task: ClientTaskConfig) -> CollectionResult | CollectionException:
        """Execute a single collection task"""
        try:
            self.platform_db.update_task_status(task.id, CollectionStatus.RUNNING)
            execution_ts = datetime.now()
            # todo...
            if task.test_data:
                db_posts = []
                for post_data in task.test_data:
                    platform_post_entry: PostEntry = self.client.raw_post_data_conversion(post_data)
                    db_post = self.client.create_post_entry(platform_post_entry, task)
                    db_posts.append(db_post)
                collection = CollectionResult(
                    posts=db_posts,
                    users=[],
                    added_posts=[],
                    task=task,
                    collected_items=len(db_posts),
                    duration=0,  # millis
                    execution_ts=execution_ts
                )
            else:
                collection = await self.client.execute_task(task)

            if isinstance(collection, CollectionResult):
                # could also be an exception...
                # test_export = json.dumps([p.content for p in collection.posts])
                self.platform_db.insert_posts(collection)
            else:
                print("handling exception")
                if isinstance(collection, QuotaExceeded):
                    print("Quota exceeded")
                    self.current_quota_halt = collection.blocked_until
                    self.platform_db.update_task_status(task.id, CollectionStatus.PAUSED)
                    store_quota(self.platform_name, self.current_quota_halt)
            return collection

        except Exception as e:
            self.platform_db.update_task_status(task.id, CollectionStatus.ABORTED)
            raise e

    def pause_running_tasks(self):
        self.platform_db.pause_running_tasks()


    @staticmethod
    def platform_tables() -> list[str]:
        return PlatformDB.platform_tables()
