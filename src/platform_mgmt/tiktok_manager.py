from src.clients.clients_models import ClientConfig
from src.clients.instances.tiktok_client import TikTokClient
from src.platform_manager import PlatformManager, T_Client


class TwitterManager(PlatformManager[TikTokClient]):


    def _create_client(self, config: ClientConfig) -> T_Client:
        pass

