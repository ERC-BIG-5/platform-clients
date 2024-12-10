"""
@deprecated replaced with manager classes

"""
import asyncio
import inspect
from typing import Optional, Type, Sequence

from typing_extensions import deprecated

from src.clients.abstract_client import AbstractClient
from src.clients.clients_models import ClientConfig
from src.clients.instances.twitter_client import TwitterClient
from src.clients.instances.youtube_client import YoutubeClient
from src.db import db_funcs
from src.db.db_funcs import main_db_get_all_platforms
from src.db.db_models import DBCollectionTask

client_classes: dict[str, Type[AbstractClient]] = {
    "twitter": TwitterClient,
    "youtube": YoutubeClient
}

"""
Loaded clients
"""
clients: dict[str, AbstractClient] = {}

@deprecated("not called")
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

@deprecated("not called")
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


@deprecated("not called")
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
