from datetime import datetime

import time

from big5_databases.databases.db_models import DBPost
from big5_databases.databases.external import CollectionStatus, ClientTaskConfig, ClientConfig
from src.clients.instances.twitter_client import TwitterClient
from src.platform_manager import PlatformManager
from tools.project_logging import get_logger


class TwitterManager(PlatformManager[TwitterClient]):
    """
    Twitter-specific platform manager that handles:
    - Twitter API client management
    - Rate limiting and quota management
    - Tweet collection and processing
    """

    def __init__(self, client_config: ClientConfig):
        super().__init__(client_config)
        self.rate_limit_window = 900  # 15 minutes in seconds
        self.rate_limit_requests = 180  # Requests per window
        self.request_timestamps: list[float] = []
        self._accounts_initialized = False
        self.logger = get_logger(__name__)

    async def _ensure_accounts_initialized(self):
        if not self._accounts_initialized:
            await self.client.initialize_auth()
            self._accounts_initialized = True

    def _create_client(self, config: ClientConfig) -> TwitterClient:
        """Create and configure Twitter client"""
        return TwitterClient(config, self)

    def _check_rate_limit(self):
        """Manage rate limiting for Twitter API"""
        current_time = time.time()

        # Remove timestamps older than the window
        self.request_timestamps = [ts for ts in self.request_timestamps
                                   if current_time - ts < self.rate_limit_window]

        # If we're at the limit, wait until we can make another request
        if len(self.request_timestamps) >= self.rate_limit_requests:
            sleep_time = self.request_timestamps[0] + self.rate_limit_window - current_time
            if sleep_time > 0:
                self.logger.info(f"Rate limit reached, waiting {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                self.request_timestamps.pop(0)

        # Add current request timestamp
        self.request_timestamps.append(current_time)

    async def process_task(self, task: ClientTaskConfig) -> list[DBPost]:
        """
        Execute Twitter collection task with specific handling for:
        - Rate limiting
        - Tweet metadata collection
        - User data collection
        """
        try:
            self.platform_db.update_task_status(task.id, CollectionStatus.RUNNING)
            start_time = datetime.now()

            await self._ensure_accounts_initialized()

            if not self.client.api:
                await self.client.initialize_auth()

            # Execute collection with rate limiting
            self._check_rate_limit()
            collected_items = await self.client.collect(
                task.collection_config
            )

            # Process results and create entries
            posts: list[DBPost] = []
            users = set()  # Use set to avoid duplicate users

            for item in collected_items:
                # Create post-entry (tweet)
                post = self.client.create_post_entry(item, task)
                posts.append(post)

                # Create user entry
                if 'user_data' in item:
                    user = self.client.create_user_entry(item['user_data'])
                    users.add(user)

            # Submit posts and users to database
            posts = self.platform_db.db_mgmt.safe_submit_posts(posts)
            # users...
            duration = (datetime.now() - start_time).total_seconds()
            self.platform_db.db_mgmt.update_task(task.id,
                                                 CollectionStatus.DONE,
                                                 len(collected_items),
                                                 len(posts),
                                                 duration)

            return posts

        except Exception as e:
            self.logger.error(f"Error executing Twitter task {task.task_name}: {str(e)}")
            self.platform_db.update_task_status(task.id, CollectionStatus.ABORTED)
            raise e

    def platform_name(self) -> str:
        return "twitter"
