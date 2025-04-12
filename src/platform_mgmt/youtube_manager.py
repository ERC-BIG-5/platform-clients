from databases.external import ClientConfig
from src.clients.instances.youtube_client import YoutubeClient, GoogleAPIKeySetting
from src.platform_manager import PlatformManager



class YoutubeManager(PlatformManager[YoutubeClient]):
    """
    YouTube-specific platform manager that handles:
    - YouTube API client management
    - Video collection and processing
    - YouTube-specific data transformations
    """

    def __init__(self, client_config: ClientConfig):
        super().__init__(client_config)

    def _create_client(self, config: ClientConfig) -> YoutubeClient:
        """Create and configure YouTube client"""
        if config and config.auth_config and 'GOOGLE_API_KEY' not in config.auth_config:
            raise ValueError("YouTube client requires GOOGLE_API_KEY in auth_config")
        else:
            config.auth_config = GoogleAPIKeySetting()
        return YoutubeClient(config, self)

    def platform_name(self) -> str:
        return "youtube"


