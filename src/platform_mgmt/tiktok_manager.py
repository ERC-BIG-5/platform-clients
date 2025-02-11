from databases.external import ClientConfig
from src.clients.instances.tiktok_client import TikTokClient
from src.platform_manager import PlatformManager, T_Client


class TikTokManager(PlatformManager[TikTokClient]):

    def __init__(self, client_config: ClientConfig):
        super().__init__(client_config)

    def _create_client(self, config: ClientConfig) -> TikTokClient:
        return TikTokClient(config, self)

    def platform_name(self) -> str:
        return "tiktok"
