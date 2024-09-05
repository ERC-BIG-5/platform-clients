from typing import Optional

from pydantic import BaseModel
from pydantic_settings import BaseSettings

from src.clients.abstract_client import AbstractClient
from src.clients.client_task import get_platforms_task_queue
from src.clients.clients_models import ClientTaskConfig
from src.clients.instances.twitter_client import TwitterClient
from src.clients.instances.youtube_client import YoutubeClient

client_classes = {
    "twitter": TwitterClient,
    "youtube": YoutubeClient
}

clients: dict[str, AbstractClient] = {}

def setup_platform(platform_name: str, config: Optional[dict | BaseModel | BaseSettings] = None) -> AbstractClient:
    client = client_classes[platform_name](config)
    clients[platform_name] = client
    return client

def get_platform_client(platform_name: str, auth_config: dict[str,str]) -> AbstractClient:
    if platform_name in clients:
        return clients[platform_name]
    else:
        return setup_platform(platform_name, auth_config)

def progress_tasks():
    platform_grouped = get_platforms_task_queue()
    for platform_name,platform_tasks in platform_grouped.items():
        task = ClientTaskConfig.model_validate(platform_tasks[0])
        client = get_platform_client(platform_name, task.auth_config)
        client.continue_task(task)


