import asyncio
from typing import Dict, Type

from sqlalchemy import exists

from src.clients.clients_models import ClientConfig, RunConfig
from src.clients.task_groups import load_tasks
from src.const import RUN_CONFIG, CLIENTS_TASKS_PATH, BIG5_CONFIG, PROCESSED_TASKS_PATH, read_run_config
from src.db.db_mgmt import DatabaseManager
from src.db.db_models import DBPlatformDatabase
from src.db.model_conversion import PlatformDatabaseModel
from src.db.platform_db_mgmt import PlatformDB
from src.platform_manager import PlatformManager
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
            self.initialize_platform_managers(None)
            self.__instance = self

    def _get_registered_platforms(self) -> list[PlatformDatabaseModel]:
        """Get all registered platforms from the main database"""
        with self.main_db.get_session() as session:
            return [o.model() for o in session.query(DBPlatformDatabase).all()]

    def initialize_platform_managers(self, platforms: list[str] = None):
        """Initialize managers for specified platforms or all registered platforms"""
        registered_platforms = self._get_registered_platforms()

        for platform_db in registered_platforms:
            platform_name = platform_db.platform
            if platforms and platform_name not in platforms:
                continue

            # Get platform-specific manager class
            manager_class = PLATFORM_MANAGERS.get(platform_name)
            if not manager_class:
                self.logger.warning(f"No manager implementation found for platform: {platform_name}")
                continue

            client_config = ClientConfig.model_validate(
                RUN_CONFIG["clients"][platform_name])

            # todo check main db, first for a default_db for platform, then use this
            if not client_config.db_config:
                client_config.db_config = PlatformDB.get_platform_default_db(platform_name)

            # Load from environment or config
            # Initialize platform manager
            try:
                manager = manager_class(
                    platform_name=platform_name,
                    client_config=client_config,
                )
                self.platform_managers[platform_name] = manager
                self.logger.debug(f"Initialized manager for platform: {platform_name}")
            except Exception as e:
                self.logger.error(f"Failed to initialize manager for {platform_name}: {str(e)}")
                raise e

    def add_platform_db(self, platform: str, connection_str: str):
        with self.main_db.get_session() as session:
            if session.query(exists().where(DBPlatformDatabase.platform == platform)).scalar():
                return
            session.add(DBPlatformDatabase(platform=platform, connection_str=connection_str))
            session.commit()

    async def progress_tasks(self, platforms: list[str] = None):
        """Progress tasks for specified platforms or all platforms"""

        # Create tasks for each platform
        platform_tasks = []
        for platform, manager in self.platform_managers.items():
            if platforms and platform not in platforms:
                continue
            platform_tasks.append(manager.process_all_tasks())

        # Execute all platform tasks concurrently
        await asyncio.gather(*platform_tasks)

    def check_new_client_tasks(self) -> list[str]:
        """
        check for json file in the specific folder and add them into the sdb
        :return: returns a list of task names
        """
        added_tasks = []
        for file in CLIENTS_TASKS_PATH.glob("*.json"):
            # create collection_task models
            group_config, tasks = load_tasks(file)
            all_added = True
            for task in tasks:
                if task.platform not in self.platform_managers:
                    self.logger.warning(f"No manager found for platform: {task.platform}")
                    all_added = False
                    continue

                manager = self.platform_managers[task.platform]
                if manager.add_task(task):
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


# Register platform-specific managers
PLATFORM_MANAGERS: Dict[str, Type[PlatformManager]] = {
    "twitter": TwitterManager,
    "youtube": YoutubeManager,
    # Add other platforms here
}
