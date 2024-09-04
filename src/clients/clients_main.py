from src.clients.abstract_client import AbstractClient
from src.clients.client_task import get_platforms_task_queue
from src.clients.instances.twitter_client import TwitterClient
from src.clients.instances.youtube_client import YoutubeClient

client_classes = {
    "twitter": TwitterClient,
    "youtube": YoutubeClient
}

clients: dict[str, AbstractClient] = {}


async def progress_tasks():
    platform_grouped = get_platforms_task_queue()


