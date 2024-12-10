from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generic, TypeVar

from src.clients.abstract_client import AbstractClient
from src.clients.clients_models import ClientConfig, ClientTaskConfig
from src.const import CollectionStatus
from src.db.db_mgmt import DatabaseManager, DatabaseConfig
from src.db.db_models import DBPost, DBCollectionTask
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

    def __init__(self, platform_name: str, db_config: DatabaseConfig, client_config: ClientConfig):
        self.platform_name = platform_name
        self.db_mgmt = DatabaseManager(db_config)
        self.client = self._create_client(client_config)
        self.client.manager = self
        self._active_tasks: list[ClientTaskConfig] = []
        self._client_setup = False
        self.logger = get_logger(__name__)

    @abstractmethod
    def _create_client(self, config: ClientConfig) -> T_Client:
        """Create platform-specific client instance"""
        pass

    def _setup_client(self):
        """Setup the client if not already set up"""
        if not self._client_setup and self.client:
            #logger.info(f"Setting up client for platform: {self.platform_name}")
            self.client.setup()
            self._client_setup = True

    def add_task(self, task: ClientTaskConfig) -> bool:
        """
        Add a new collection task
        Returns True if task was added, False if it already exists
        """
        with self.db_mgmt.get_session() as session:
            if session.query(DBCollectionTask).filter_by(task_name=task.task_name).first():
                return False

            db_task = DBCollectionTask(
                task_name=task.task_name,
                platform=self.platform_name,
                collection_config=task.collection_config.model_dump(),
                status=CollectionStatus.INIT,
                time_added=datetime.now()
            )
            session.add(db_task)
            return True

    def get_pending_tasks(self) -> list[ClientTaskConfig]:
        """Get all tasks that need to be executed"""
        with self.db_mgmt.get_session() as session:
            tasks = session.query(DBCollectionTask).filter(
                DBCollectionTask.status.in_([
                    CollectionStatus.INIT,
                    CollectionStatus.ACTIVE,
                    CollectionStatus.PAUSED
                ])
            ).all()
            return [ClientTaskConfig.model_validate(task) for task in tasks]

    async def execute_task(self, task: ClientTaskConfig) -> list[DBPost]:
        """Execute a single collection task"""
        try:
            self._update_task_status(task.id, CollectionStatus.RUNNING)

            # Execute collection
            start_time = datetime.now()
            collected_items = await self.client.collect(
                task.collection_config,
                task.collection_config
            )

            # Process results
            posts = [
                self.client.create_post_entry(item, task)
                for item in collected_items
            ]

            # Store posts
            with self.db_mgmt.get_session() as session:
                session.add_all(posts)

                # Update task status
                duration = (datetime.now() - start_time).total_seconds()
                task_record = session.query(DBCollectionTask).get(task.id)
                task_record.status = CollectionStatus.DONE
                task_record.found_items = len(collected_items)
                task_record.added_items = len(posts)
                task_record.collection_duration = int(duration * 1000)

            return posts

        except Exception as e:
            self._update_task_status(task.id, CollectionStatus.ABORTED)
            raise e

    def _update_task_status(self, task_id: int, status: CollectionStatus):
        """Update task status in database"""
        with self.db_mgmt.get_session() as session:
            task = session.query(DBCollectionTask).get(task_id)
            task.status = status
            session.commit()

    async def process_all_tasks(self):
        """Process all pending tasks"""
        tasks = self.get_pending_tasks()
        for task in tasks:
            self._setup_client()
            await self.execute_task(task)
            pass