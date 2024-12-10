from src.clients.clients_models import ClientConfig
from src.clients.instances.youtube_client import YoutubeClient


def test_basic():
    YoutubeClient(ClientConfig())