from src.clients.clients_models import ClientConfig
from src.clients.instances.tiktok_client import TikTokClient
from src.platform_manager import PlatformManager, T_Client


class TikTokManager(PlatformManager[TikTokClient]):


    def __init__(self, config:ClientConfig, **kwargs):
        super().__init__("tiktok", config, **kwargs)

    def _create_client(self, config: ClientConfig) -> TikTokClient:
        return TikTokClient(config)

    def platform_name(self) -> str:
        return "tiktok"