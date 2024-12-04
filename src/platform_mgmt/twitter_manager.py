from datetime import datetime

import time

# from src.clients.instances.twitter_client import TwitterClient
from src.clients.clients_models import ClientConfig, ClientTaskConfig
from src.clients.instances.twitter_client_gen import TwitterClient
from src.const import CollectionStatus
from src.db.db_models import DBPost, DBCollectionTask
from src.misc.project_logging import get_b5_logger
from src.platform_manager import PlatformManager

logger = get_b5_logger(__file__)


class TwitterManager(PlatformManager[TwitterClient]):
    """
    Twitter-specific platform manager that handles:
    - Twitter API client management
    - Rate limiting and quota management
    - Tweet collection and processing
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rate_limit_window = 900  # 15 minutes in seconds
        self.rate_limit_requests = 180  # Requests per window
        self.request_timestamps: list[float] = []
        self._accounts_initialized = False


    async def _ensure_accounts_initialized(self):
        if not self._accounts_initialized:
            await self.client.initialize_auth()
            self._accounts_initialized = True

    def _create_client(self, config: ClientConfig) -> TwitterClient:
        """Create and configure Twitter client"""
        if config and config.auth_config and not all(k in config.auth_config for k in
                                             ['TWITTER_API_KEY', 'TWITTER_API_SECRET']):
            raise ValueError("Twitter client requires TWITTER_API_KEY and TWITTER_API_SECRET in auth_config")
        return TwitterClient(config)

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
                logger.info(f"Rate limit reached, waiting {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
                self.request_timestamps.pop(0)

        # Add current request timestamp
        self.request_timestamps.append(current_time)

    async def execute_task(self, task: ClientTaskConfig) -> list[DBPost]:
        """
        Execute Twitter collection task with specific handling for:
        - Rate limiting
        - Tweet metadata collection
        - User data collection
        """
        try:
            self._update_task_status(task.id, CollectionStatus.RUNNING)
            start_time = datetime.now()

            await self._ensure_accounts_initialized()
            # Twitter-specific config transformation
            twitter_config = self.client.transform_config(task.collection_config)

            # Execute collection with rate limiting
            self._check_rate_limit()
            collected_items = await self.client.collect(
                twitter_config,
                task.collection_config
            )

            # Process results and create entries
            posts: list[DBPost] = []
            users = set()  # Use set to avoid duplicate users

            for item in collected_items:
                # Create post entry (tweet)
                post = self.client.create_post_entry(item, task)
                posts.append(post)

                # Create user entry
                if 'user_data' in item:
                    user = self.client.create_user_entry(item['user_data'])
                    users.add(user)

            # Store in database
            with self.db_mgmt.get_session() as session:
                # Add users first to establish relationships
                session.add_all(users)
                session.flush()

                # Add posts
                session.add_all(posts)

                # Update task status
                duration = (datetime.now() - start_time).total_seconds()
                task_record = session.query(DBCollectionTask).get(task.id)
                task_record.status = CollectionStatus.DONE
                task_record.found_items = len(collected_items)
                task_record.added_items = len(posts)
                task_record.collection_duration = int(duration * 1000)

                session.commit()

            return posts

        except Exception as e:
            logger.error(f"Error executing Twitter task {task.task_name}: {str(e)}")
            self._update_task_status(task.id, CollectionStatus.ABORTED)
            raise e
