import asyncio
from asyncio import Task
from pathlib import Path
from typing import Type, Optional, TypeVar, TypedDict

import sys

from big5_databases.databases.db_models import CollectionResult
from big5_databases.databases.external import DBConfig, ClientConfig
from big5_databases.databases.meta_database import MetaDatabase
from src.clients.abstract_client import ConcreteClientClass
from src.clients.clients_models import RunConfig
from src.const import BIG5_CONFIG, read_run_config
from src.platform_manager import PlatformManager
from src.task_manager import TaskManager
from tools.project_logging import get_logger

logger = get_logger(__file__)

platform_results = TypedDict("platform_results", {"task_names": list[str], "num_posts_added": int})


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

    def __init__(self, meta_db_path: Optional[Path | str] = None):
        # self.logger = get_logger(__file__)
        if not self.__instance:
            self.platform_managers: dict[str, PlatformManager] = {}
            self.run_config = RunConfig.model_validate(read_run_config())
            try:
                self.main_db = MetaDatabase() # DatabaseManager.sqlite_db_from_path(BASE_DATA_PATH / "dbs/main.sqlite")
            except ValueError as e:
                logger.error(e)
                logger.error("Run command 'init' (typer src/main.py run init)")
                exit(1)
            self.logger = get_logger(__name__)
            self.initialize_platform_managers()
            self.__instance = self
            self.current_tasks: list[tuple[str, Task]] = []  # platform_name, python async-task
            self.task = TaskManager(self)


    def initialize_platform_managers(self, config: Optional[RunConfig] = None):
        """Initialize managers for specified platforms or all registered platforms"""
        if not config:
            config = self.run_config

        registered_platforms = self.main_db.get_dbs()

        for platform in config.clients:
            # todo, dbs should have a name in the yaml
            if platform not in [p.platform for p in registered_platforms]:
                self.add_platform_db(platform, config.clients[platform].db_config)

            client_config = config.clients[platform]

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
                platform_manager.active = self.run_config.clients[platform].progress
            else:
                logger.info(f"Cannot initialize platform {platform}")
                continue
            logger.debug(f"Initialized manager for platform: {platform}; active: {platform_manager.active}")

    def add_platform_db(self, platform: str, db_config: DBConfig):
        self.main_db.add_db(platform, db_config)


    async def progress_tasks(self) -> dict[str, platform_results]:
        """
        Progress tasks for specified platforms or all platforms
        returns {<platform_name>: [<task_name>, ...], ...}
        """
        # Create tasks for each platform
        for platform, manager in self.platform_managers.items():
            if not manager.active:
                logger.debug(f"Progress for platform: '{platform}' deactivated")
                continue
            coro_task = asyncio.create_task(manager.process_all_tasks())
            self.current_tasks.append((manager.platform_name, coro_task))
        # Execute all platform tasks concurrently
        res: list[list[CollectionResult]] = await asyncio.gather(*[t for platform, t in self.current_tasks])
        # convert to result
        result: dict[str, platform_results] = {}
        for platform_res, exec_task in zip(res, self.current_tasks):
            platform_tasks_results: platform_results = {"task_names": [], "num_posts_added": 0}
            for col_res in platform_res:
                platform_tasks_results["task_names"].append(col_res.task.task_name)
                platform_tasks_results["num_posts_added"] += len(col_res.added_posts)
            result[exec_task[0]] = platform_tasks_results
        self.current_tasks.clear()
        return dict(result)

    async def abort_tasks(self):
        for task_coro in self.current_tasks:
            task_coro[1].cancel()
            # self.platform_managers.items()
            # task.platform

    def get_status(self) -> dict[str, dict[str, str | bool]]:
        return {p_n: {"currently running": platform.status.name,
                      "active": platform.active} for p_n, platform in self.platform_managers.items()}

    async def collect(self):
        try:
            self.task.check_new_client_tasks()
            self.task.fix_tasks()
            # Progress all tasks
            res = await self.progress_tasks()
            # todo, can go into progress_tasks
            self.logger.info({p: t for p, t in res.items()})
            self.logger.debug(res)
        except KeyboardInterrupt:
            asyncio.run(self.abort_tasks())
            print("bye bye")
            sys.exit(0)
        except Exception as e:
            get_logger(__name__).error(f"Error in main program flow: {str(e)}")
            raise

    async def run_collect_loop(self):
        try:
            while True:
                await self.collect()
                await asyncio.sleep(BIG5_CONFIG.main_loop_sleep_interval)
        except KeyboardInterrupt:
            asyncio.run(self.abort_tasks())
            print("bye bye")
            sys.exit(0)
        except Exception as e:
            get_logger(__name__).error(f"Error in main program flow: {str(e)}")
            raise


T = TypeVar('T', bound=PlatformManager)


def get_client_class(platform: str) -> Optional[Type[ConcreteClientClass]]:
    match platform:
        case "tiktok":
            try:
                from src.clients.instances.tiktok_client import TikTokClient
                return TikTokClient
            except ModuleNotFoundError as err:
                print(err)
                print("You might want to run `uv sync --extra tiktok'")
        case "twitter":
            try:
                from src.clients.instances.twitter_client import TwitterClient
                return TwitterClient
            except ModuleNotFoundError as err:
                print(err)
                print("You might want to run `uv sync --extra twitter'")
        case "youtube":
            try:
                from src.clients.instances.youtube_client import YoutubeClient
                return YoutubeClient
            except ModuleNotFoundError as err:
                print(err)
                print("You might want to run `uv sync --extra youtube'")
        case _:
            print(f"Platform '{platform}' not supported")
            return None


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
