import asyncio
import inspect
from pathlib import Path
from typing import Optional, Type, Sequence

from pydantic_core import ValidationError

from src.clients.abstract_client import AbstractClient
from src.clients.clients_models import ClientTaskConfig, ClientConfig, ClientTaskGroupConfig
from src.clients.instances.twitter_client import TwitterClient
from src.clients.instances.youtube_client import YoutubeClient
from src.clients.task_groups import generate_configs
from src.const import CLIENTS_TASKS_PATH, BIG5_CONFIG, PROCESSED_TASKS_PATH
from src.db import db_funcs
from src.db.db_funcs import main_db_add_new_db, main_db_get_all_platforms
from src.db.db_mgmt import DatabaseManager, DatabaseConfig
from src.db.db_models import DBCollectionTask
from src.db.platform_db_mgmt import PlatformDB
from src.misc.files import get_abs_path, read_data
from src.misc.project_logging import get_b5_logger

logger = get_b5_logger(__file__)

client_classes: dict[str, Type[AbstractClient]] = {
    "twitter": TwitterClient,
    "youtube": YoutubeClient
}

"""
Loaded clients
"""
clients: dict[str, AbstractClient] = {}


def setup_client(platform_name: str,
                 config: Optional[ClientConfig] = None) -> AbstractClient:
    """
    Initiate and set up a client for a specific platform
    :param platform_name: name of the platform
    :param config: config for credentials and so on
    :return: a client object
    """
    client = client_classes[platform_name](config)
    clients[platform_name] = client
    if inspect.iscoroutinefunction(client.setup):
        asyncio.run(client.setup())
    else:
        client.setup()
    return client


def get_platform_client(platform_name: str, config: ClientConfig) -> AbstractClient:
    """
    Get existing client or create a new one
    :param platform_name:
    :param config:
    :return:
    """
    if platform_name in clients:
        return clients[platform_name]
    else:
        # todo, catch missing platform
        return setup_client(platform_name, config)

def load_tasks(task_path: Path) -> list[ClientTaskConfig]:
    """
    Load an validate a task file
    :param task_path: absolute or relative path (to CLIENTS_TASKS_PATH)
    :return: task object
    """
    abs_task_path = get_abs_path(task_path, CLIENTS_TASKS_PATH)
    data = read_data(abs_task_path)
    ct_cfg_err = None
    try:
        return [ClientTaskConfig.model_validate(data)]
    except ValidationError as v_err:
        ct_cfg_err = v_err

    try:
        ctg = ClientTaskGroupConfig.model_validate(data)
        return generate_configs(ctg)
    except ValidationError as v_err:
        print(ct_cfg_err)
        print("****")
        print(v_err)
        logger.error("Task file cannot be parsed neither as TaskConfig nor as TaskGroupConfig")
        return []

def check_new_client_tasks() -> list[str]:
    """
    check for json file in the specific folder and add them into the sdb
    :return: returns a list of task names
    """
    added_task = []
    for file in CLIENTS_TASKS_PATH.glob("*.json"):
        # create collection_task models
        tasks = load_tasks(file)
        all_added = True
        for task in tasks:
            platform_db_mgmt = PlatformDB(task.platform)
            processed = platform_db_mgmt.add_db_collection_task(task)
            if processed:
                added_task.append(task.task_name)
            else:
                all_added = False

            main_db_add_new_db(task.platform, platform_db_mgmt.db_config.connection_string)

        # todo only move added tasks?
        if all_added and BIG5_CONFIG.moved_processed_tasks:
            file.rename(PROCESSED_TASKS_PATH / file.name)
        else:
            logger.warning(f"task of file exists already: {file.name}")
    logger.info(f"new tasks: # {len(added_task)}")
    logger.debug(f"new tasks: # {[t for t in  added_task]}")
    return added_task


def get_platforms_task_queues(platforms: Optional[Sequence[str]] = None) -> dict[str, list[DBCollectionTask]]:
    """
    Get for all given platforms all tasks
    todo: dont use the db model class...
    :param platforms:
    :return: a dict {platform_name: list[task]}
    """
    platforms_d_models = main_db_get_all_platforms()
    for platform in platforms_d_models:
        if platform not in platforms:
            continue



    tasks = db_funcs.get_task_queue(platforms)
    platform_grouped: dict[str, list[DBCollectionTask]] = {}
    for task in tasks:
        platform_grouped.setdefault(task.platform, []).append(task)
    return platform_grouped
