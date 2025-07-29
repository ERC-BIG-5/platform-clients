from big5_databases.databases.external import CollectConfig
from src.clients.clients_models import ClientConfig
from src.clients.instances.youtube_client import YoutubeClient, YoutubeSearchParameters


def test_basic():
    YoutubeClient(ClientConfig())

def test_time_validator():
    conf = {
        "platform": "tiktok",
        "limit": 20,
        "from_time": "2023-01-02",
        "to_time": "2023-12-31"
    }
    conf_ = YoutubeSearchParameters.model_validate(conf)