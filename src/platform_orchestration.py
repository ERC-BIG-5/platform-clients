import asyncio
from typing import Dict, Type

from src.clients.clients_models import ClientConfig
from src.const import RUN_CONFIG
from src.db.db_mgmt import DatabaseManager, DatabaseConfig
from src.db.db_models import DBPlatformDatabase
from src.db.model_conversion import PlatformDatabaseModel
from src.misc.project_logging import get_b5_logger
from src.platform_manager import PlatformManager
from src.platform_mgmt.twitter_manager import TwitterManager
from src.platform_mgmt.youtube_manager import YoutubeManager

logger = get_b5_logger(__file__)


class PlatformOrchestrator:
    """
    Central orchestrator that manages all platform operations.
    Handles platform discovery, initialization, and task execution.
    """

    def __init__(self):
        self.platform_managers: Dict[str, PlatformManager] = {}
        self.main_db = DatabaseManager(DatabaseConfig.get_main_db_config())

    def _get_registered_platforms(self) -> list[PlatformDatabaseModel]:
        """Get all registered platforms from the main database"""
        with self.main_db.get_session() as session:
            return [o.model() for o in session.query(DBPlatformDatabase).all()]

    def initialize_platform_managers(self, platforms: list[str] = None):
        """Initialize managers for specified platforms or all registered platforms"""
        registered_platforms = self._get_registered_platforms()

        for platform_db in registered_platforms:
            platform_name = platform_db.platform
            if platforms and platform_name not in platforms:
                continue

            # Get platform-specific manager class
            manager_class = PLATFORM_MANAGERS.get(platform_name)
            if not manager_class:
                logger.warning(f"No manager implementation found for platform: {platform_name}")
                continue

            # Create database config for platform
            db_config = DatabaseConfig(
                db_type="sqlite",  # Or get from config
                connection_string=platform_db.connection_str
            )

            # todo. bring back
            # Create client config for platform
            # todo: does it always need to be there
            # bind configs to tasks?
            client_config = ClientConfig.model_validate(
                RUN_CONFIG["clients"][platform_name])  # Load from environment or config
            # Initialize platform manager
            try:
                manager = manager_class(
                    platform_name=platform_name,
                    db_config=db_config,
                    client_config=client_config,
                )
                self.platform_managers[platform_name] = manager
                logger.info(f"Initialized manager for platform: {platform_name}")
            except Exception as e:
                raise e
                logger.error(f"Failed to initialize manager for {platform_name}: {str(e)}")

    async def progress_tasks(self, platforms: list[str] = None):
        """Progress tasks for specified platforms or all platforms"""
        if not self.platform_managers:
            self.initialize_platform_managers(platforms)

        # Create tasks for each platform
        platform_tasks = []
        for platform, manager in self.platform_managers.items():
            if platforms and platform not in platforms:
                continue
            platform_tasks.append(manager.process_all_tasks())

        # Execute all platform tasks concurrently
        await asyncio.gather(*platform_tasks)


# Register platform-specific managers
PLATFORM_MANAGERS: Dict[str, Type[PlatformManager]] = {
    "twitter": TwitterManager,
    "youtube": YoutubeManager,
    # Add other platforms here
}
