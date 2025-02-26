import asyncio
from asyncio import Task
from pathlib import Path
from typing import Type, Optional, TypeVar

from sqlalchemy import exists

from databases.db_mgmt import DatabaseManager
from databases.db_models import DBPlatformDatabase
from databases.external import DBConfig, ClientConfig
from databases.model_conversion import PlatformDatabaseModel
from src.clients.clients_models import RunConfig
from src.clients.task_groups import load_tasks
from src.const import RUN_CONFIG, CLIENTS_TASKS_PATH, BIG5_CONFIG, PROCESSED_TASKS_PATH, read_run_config
from src.misc.platform_quotas import load_quotas
from src.platform_manager import PlatformManager
from src.platform_mgmt.tiktok_manager import TikTokManager
from src.platform_mgmt.twitter_manager import TwitterManager
from src.platform_mgmt.youtube_manager import YoutubeManager
from tools.project_logging import get_logger


class PlatformOrchestrator:
    """
    Central orchestrator that manages all platform operations.
    Handles platform discovery, initialization, and task execution.
    """
    __instance: "PlatformOrchestrator" = None

    def __new__(cls, *args, **kwargs):
        if cls.__instance:
            return cls.__instance
        return super().__new__(cls)

    def __init__(self):
        if not self.__instance:
            self.platform_managers: dict[str, PlatformManager] = {}
            self.run_config = RunConfig.model_validate(read_run_config())
            self.main_db = DatabaseManager(DatabaseManager.get_main_db_config())
            self.logger = get_logger(__name__)
            self.initialize_platform_managers()
            self.__instance = self
            self.current_tasks: list[Task] = []

    def _get_registered_platforms(self) -> list[PlatformDatabaseModel]:
        """Get all registered platforms from the main database"""
        with self.main_db.get_session() as session:
            return [o.model() for o in session.query(DBPlatformDatabase).all()]

    def initialize_platform_managers(self, config: Optional[RunConfig] = None):

        """Initialize managers for specified platforms or all registered platforms"""
        if not config:
            config = self.run_config

        registered_platforms = self._get_registered_platforms()

        for platform in config.clients:
            if platform not in [p.platform for p in registered_platforms]:
                self.add_platform_db(platform, config.clients[platform].db_config)
                # self.logger.warning(f"Platform '{platform}' not registered in main database")

            manager_class = PLATFORM_MANAGERS.get(platform)
            if not manager_class:
                self.logger.error(
                    f"No manager implementation found for platform: {platform}. "
                    f"Add it to the platform_orchestration.py file: 'PLATFORM_MANAGERS'")

            client_config = ClientConfig.model_validate(
                RUN_CONFIG["clients"][platform])

            # todo check main db, first for a default_db for platform, then use this
            # if not client_config.db_config:
            #     client_config.db_config = PlatformDB.get_platform_default_db(platform)

            # Load from environment or config
            # Initialize platform manager
            platform_quotas = load_quotas()
            try:

                manager = manager_class(client_config)
                self.platform_managers[platform] = manager
                self.logger.debug(f"Initialized manager for platform: {platform}")
                if manager.platform_name() in platform_quotas:
                    manager.current_quota_halt = platform_quotas[manager.platform_name()]
                    self.logger.info(
                        f"{manager.platform_name()} has a halting quota until {manager.current_quota_halt}")
            except Exception as e:
                self.logger.error(f"Failed to initialize manager for {platform}: {str(e)}")
                raise e

    def add_platform_db(self, platform: str, db_config: DBConfig):
        with self.main_db.get_session() as session:
            if session.query(exists().where(DBPlatformDatabase.platform == platform)).scalar():
                return
            session.add(DBPlatformDatabase(platform=platform, connection_str=db_config.connection_str))
            session.commit()

    async def progress_tasks(self, platforms: list[str] = None):
        """Progress tasks for specified platforms or all platforms"""

        # Create tasks for each platform
        # platform_tasks = []
        for platform, manager in self.platform_managers.items():
            if not self.run_config.clients[platform].progress:
                self.logger.info(f"Progress for platform: '{platform}' deactivated")
                continue
            if halt_until := manager.has_quota_halt():
                self.logger.info(
                    f"Progress for platform: '{platform}' deactivated due to quota halt, {halt_until:%Y.%m.%d - %H:%M}")
                continue
            if platforms and platform not in platforms:
                self.logger.warning(f"undefined platform: '{platform}'")
                continue
            coro_task = asyncio.create_task(manager.process_all_tasks())
            self.current_tasks.append(coro_task)
        # Execute all platform tasks concurrently
        await asyncio.gather(*self.current_tasks)

    def check_new_client_tasks(self, task_dir: Optional[Path] = None) -> list[str]:
        """
        check for json file in the specific folder and add them into the sdb
        :return: returns a list of task names
        """
        added_tasks:list[str] = []
        missing_platform_managers: set[str] = set()

        files = []
        if not task_dir:
            task_dir = CLIENTS_TASKS_PATH
        else:
            files.append(task_dir)
        if task_dir.is_dir():
            files = task_dir.glob("*.json")

        for file in files:
            # create collection_task models
            task_group, tasks = load_tasks(file)
            all_added = True
            for task in tasks:
                if task.platform in missing_platform_managers:
                    continue
                if task.platform not in self.platform_managers:
                    self.logger.warning(f"No manager found for platform: {task.platform}")
                    all_added = False
                    missing_platform_managers.add(task.platform)
                    continue

                manager = self.platform_managers[task.platform]
                if manager.add_task(task, task_group):
                    added_tasks.append(task.task_name)
                    # Register platform database in main DB
                    self.add_platform_db(task.platform, manager.platform_db.db_config.connection_str)
                else:
                    all_added = False

            # todo only move added tasks?
            if all_added and BIG5_CONFIG.moved_processed_tasks:
                file.rename(PROCESSED_TASKS_PATH / file.name)
            # else:
            #    self.logger.warning(f"task of file exists already: {file.name}")
        self.logger.info(f"new tasks: # {len(added_tasks)}")
        self.logger.debug(f"new tasks: # {[t for t in added_tasks]}")
        return added_tasks

    def fix_tasks(self):
        """
        set the tasks of status "RUNNING" to "PAUSED"
        """
        for platform, platform_mgmt in self.platform_managers.items():
            platform_mgmt.pause_running_tasks()

    async def abort_tasks(self):
        for task_coro in self.current_tasks:
            task_coro.cancel()
            # self.platform_managers.items()
            # task.platform


T = TypeVar('T', bound=PlatformManager)
# Register platform-specific managers
PLATFORM_MANAGERS: dict[str, Type[T]] = {
    "twitter": TwitterManager,
    "youtube": YoutubeManager,
    "tiktok": TikTokManager
    # Add other platforms here
}
