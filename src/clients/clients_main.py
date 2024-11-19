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
from src.db.db_models import DBCollectionTask
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


def progress_tasks(platforms: list[str] = None) -> None:
    """
    Progress all tasks specific (or all) clients
    :param platforms: Select the clients (or leave None for all)
    TODO, this is synchronously, which we dont want in the end
    :return:
    """
    logger.debug("progressing client tasks")
    platform_grouped = get_platforms_task_queues(platforms)
    for platform_name, platform_tasks in platform_grouped.items():
        # this is just adding one at a time
        # convert to platform specific task
        tasks = [ClientTaskConfig.model_validate(db_task, from_attributes=True)
                 for db_task in platform_tasks]
        client = get_platform_client(platform_name, tasks[0].client_config)
        client.add_tasks(tasks)
        client.continue_tasks()


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
        return generate_configs(ClientTaskGroupConfig.model_validate(data))
    except ValidationError as v_err:
        print(ct_cfg_err)
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
        tasks = load_tasks(file)
        all_added = True
        for task in tasks:
            processed = db_funcs.add_db_collection_task(task)
            if processed:
                added_task.append(task.task_name)
            else:
                all_added = False

        # todo only move added tasks?
        if all_added and BIG5_CONFIG.moved_processed_tasks:
            file.rename(PROCESSED_TASKS_PATH / file.name)
        else:
            logger.debug(f"task of file exists already: {file.name}")
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
    tasks = db_funcs.get_task_queue(platforms)
    platform_grouped: dict[str, list[DBCollectionTask]] = {}
    for task in tasks:
        platform_grouped.setdefault(task.platform, []).append(task)
    return platform_grouped
