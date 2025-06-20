import asyncio
from asyncio import Task
from collections import defaultdict
from pathlib import Path
from typing import Type, Optional, TypeVar

from sqlalchemy import exists

from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_models import DBPlatformDatabase
from big5_databases.databases.external import DBConfig, ClientConfig, ClientTaskConfig
from big5_databases.databases.model_conversion import PlatformDatabaseModel
from src.clients.clients_models import RunConfig, ClientTaskGroupConfig
from src.clients.task_groups import load_tasks
from src.const import RUN_CONFIG, CLIENTS_TASKS_PATH, BIG5_CONFIG, PROCESSED_TASKS_PATH, read_run_config, BASE_DATA_PATH
from src.misc.platform_quotas import load_quotas
from src.platform_manager import PlatformManager, PlatformStatus

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
        self.logger = get_logger(__file__)
        if not self.__instance:
            self.platform_managers: dict[str, PlatformManager] = {}
            self.run_config = RunConfig.model_validate(read_run_config())
            try:
                self.main_db = DatabaseManager.sqlite_db_from_path(BASE_DATA_PATH / "dbs/main.sqlite")
            except ValueError as e:
                self.logger.error(e)
                self.logger.error("Run command 'init' (typer src/main.py run init)")
                exit(1)
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

            """
            manager_class = platform_managers.get(platform)
            if not manager_class:
                self.logger.error(
                    f"No manager implementation found for platform: '{platform}'.")
                continue
            """
            client_config = ClientConfig.model_validate(
                RUN_CONFIG["clients"][platform])

            client_config.db_config.tables = PlatformManager.platform_tables()

            # add client tables

            # todo check main db, first for a default_db for platform, then use this
            # if not client_config.db_config:
            #     client_config.db_config = PlatformDB.get_platform_default_db(platform)

            # Load from environment or config
            # Initialize platform manager

            platform_manager = get_platform_manager(platform, client_config)
            if platform_manager:
                self.platform_managers[platform] = platform_manager
            self.logger.debug(f"Initialized manager for platform: {platform}")

    def add_platform_db(self, platform: str, db_config: DBConfig):
        with self.main_db.get_session() as session:
            if session.query(exists().where(DBPlatformDatabase.platform == platform)).scalar():
                return
            session.add(DBPlatformDatabase(platform=platform,
                                           db_path=db_config.db_connection.db_path.as_posix(), is_default=True))
            session.commit()

    async def progress_tasks(self, platforms: list[str] = None):
        """Progress tasks for specified platforms or all platforms"""
        # Create tasks for each platform
        # platform_tasks = []
        for platform, manager in self.platform_managers.items():
            if platforms and platform not in platforms:
                continue
            if not self.run_config.clients[platform].progress:
                self.logger.info(f"Progress for platform: '{platform}' deactivated")
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
        files = []
        if not task_dir:
            task_dir = CLIENTS_TASKS_PATH
        else:
            files.append(task_dir)
        if task_dir.is_dir():
            files = task_dir.glob("*.json")

        added_tasks = []
        for file in files:
            # create collection_task models
            added_tasks.extend(self.handle_task_file(file))
        return added_tasks
    
    def handle_task_file(self, file: Path) -> list[str]:
        tasks, task_group = load_tasks(file)
        added_tasks, all_added = self.process_tasks(tasks, task_group)

        if all_added and BIG5_CONFIG.moved_processed_tasks:
            file.rename(PROCESSED_TASKS_PATH / file.name)

        self.logger.info(f"new tasks: # {len(added_tasks)}")
        self.logger.debug(f"new tasks: # {[t for t in added_tasks]}")
        return added_tasks

    def process_tasks(self,
                      tasks: list[ClientTaskConfig],
                      task_group: Optional[ClientTaskGroupConfig] = None) -> tuple[list[str], bool]:
        """

        @return: list of task names and if all tasks were added
        """
        added_tasks: list[str] = []
        missing_platform_managers: set[str] = set()

        all_added = True
        grouped_by_platform = defaultdict(list)

        for task in tasks:
            if task.platform in missing_platform_managers:
                all_added = False
                continue
            if task.platform not in self.platform_managers:
                self.logger.warning(f"No manager found for platform: {task.platform}")
                all_added = False
                missing_platform_managers.add(task.platform)
                continue

            grouped_by_platform[task.platform].append(task)

        for group, g_tasks in grouped_by_platform.items():
            manager = self.platform_managers[group]
            added_tasks_names = manager.add_tasks(g_tasks)
            added_tasks.extend(added_tasks_names)
            if len(g_tasks) != len(added_tasks_names):
                self.logger.warning(f"Not all tasks added for platform: {task.platform}, {len(added_tasks_names)}/{len(g_tasks)}")
                # Register platform database in main DB
                #self.add_platform_db(task.platform, manager.platform_db.db_config.connection_str)
                all_added = False

        return added_tasks, all_added


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

    def get_status(self) -> dict[str, str]:
        return {p_n: platform.status.name for p_n, platform in self.platform_managers.items()}

T = TypeVar('T', bound=PlatformManager)


def get_platform_manager(platform: str, client_config: ClientConfig) -> Optional[PlatformManager]:
    match platform:
        case "tiktok":
            try:
                from src.clients.instances.tiktok_client import TikTokClient
                return PlatformManager(platform, TikTokClient, client_config)
            except ModuleNotFoundError as err:
                print(err)
                print("You might want to run `uv sync --extra tiktok'")
        case "twitter":
            try:
                from src.clients.instances.twitter_client import TwitterClient
                return PlatformManager(platform, TwitterClient, client_config)
            except ModuleNotFoundError as err:
                print(err)
                print("You might want to run `uv sync --extra twitter'")
        case "youtube":
            try:
                from src.clients.instances.youtube_client import YoutubeClient
                return PlatformManager(platform, YoutubeClient, client_config)
            except ModuleNotFoundError as err:
                print(err)
                print("You might want to run `uv sync --extra youtube'")
        case _:
            print(f"Platform '{platform}' not supported")
            return None
