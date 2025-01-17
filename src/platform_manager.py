from abc import ABC, abstractmethod
from asyncio import sleep
from random import randint
from typing import Generic, TypeVar

from databases.db_models import CollectionResult
from databases.external import CollectionStatus
from databases.platform_db_mgmt import PlatformDB
from src.clients.abstract_client import AbstractClient
from src.clients.clients_models import ClientConfig, ClientTaskConfig
from tools.project_logging import get_logger

T_Client = TypeVar('T_Client', bound=AbstractClient)


class PlatformManager(Generic[T_Client], ABC):
    """
    Base class for managing platform-specific operations including:
    - Client management
    - Database operations
    - Task queue management

    Each platform should implement its own subclass of PlatformManager.
    """

    def __init__(self, platform_name: str, client_config: ClientConfig):
        self.platform_name = platform_name
        self.client = self._create_client(client_config)

        # Initialize platform database
        self.platform_db = PlatformDB(platform_name, client_config.db_config)
        self.client.manager = self
        self._active_tasks: list[ClientTaskConfig] = []
        self._client_setup = False
        self.logger = get_logger(__name__)

    @abstractmethod
    def _create_client(self, config: ClientConfig) -> T_Client:
        """Create platform-specific client instance"""
        pass

    def _setup_client(self):
        """Set up the client if not already set up"""
        if not self._client_setup and self.client:
            # logger.info(f"Setting up client for platform: {self.platform_name}")
            self.client.setup()
            self._client_setup = True

    def add_task(self, task: ClientTaskConfig) -> bool:
        """Add a new collection task"""
        return self.platform_db.add_db_collection_task(task)

    def get_pending_tasks(self) -> list[ClientTaskConfig]:
        """Get all tasks that need to be executed"""
        return self.platform_db.get_pending_tasks()

    async def process_all_tasks(self):
        """Process all pending tasks"""
        tasks = self.get_pending_tasks()
        self.logger.info(f"Task queue: {len(tasks)}")
        self._setup_client()
        for task in tasks:
            await self.process_task(task)
            sleep_time = self.client.config.request_delay
            sleep_time += randint(0, self.client.config.delay_randomize)
            await sleep(sleep_time)

    async def process_task(self, task: ClientTaskConfig) -> CollectionResult:
        """Execute a single collection task"""
        try:
            self.platform_db.update_task_status(task.id, CollectionStatus.RUNNING)
            collection = await self.client.execute_task(task)
            self.platform_db.insert_posts(collection)
            return collection

        except Exception as e:
            self.platform_db.update_task_status(task.id, CollectionStatus.ABORTED)
            raise e
