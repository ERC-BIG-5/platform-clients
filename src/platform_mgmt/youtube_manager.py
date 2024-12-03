from typing import Any, List
from datetime import datetime

from src.clients.instances.youtube_client import YoutubeClient
from src.clients.clients_models import ClientConfig, CollectConfig, ClientTaskConfig
from src.const import CollectionStatus
from src.db.db_models import DBPost, DBUser, DBCollectionTask
from src.misc.project_logging import get_b5_logger
from src.platform_manager import PlatformManager

logger = get_b5_logger(__file__)


class YoutubeManager(PlatformManager[YoutubeClient]):
    """
    YouTube-specific platform manager that handles:
    - YouTube API client management
    - Video collection and processing
    - YouTube-specific data transformations
    """

    def _create_client(self, config: ClientConfig) -> YoutubeClient:
        """Create and configure YouTube client"""
        # todo
        if config and config.auth_config and 'GOOGLE_API_KEY' not in config.auth_config:
            raise ValueError("YouTube client requires GOOGLE_API_KEY in auth_config")
        return YoutubeClient(config)

    async def execute_task(self, task: ClientTaskConfig) -> list[DBPost]:
        """
        Execute YouTube collection task with specific handling for:
        - Quota management
        - Video metadata collection
        - Comment collection (if configured)
        """
        try:
            self._update_task_status(task.id, CollectionStatus.RUNNING)
            start_time = datetime.now()

            # YouTube-specific config transformation
            yt_config = self.client.transform_config(task.collection_config)

            # Execute collection with quota awareness
            collected_items = await self.client.collect(
                yt_config,
                task.collection_config
            )

            # Process results and create post entries
            posts: list[DBPost] = []
            users: list[DBUser] = set()  # Use set to avoid duplicate channels

            for item in collected_items:
                # Create post entry (video)
                post = self.client.create_post_entry(item, task)
                posts.append(post)

                # Create user entry (channel)
                if 'channel_data' in item:
                    user = self.client.create_user_entry(item['channel_data'])
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
            logger.error(f"Error executing YouTube task {task.task_name}: {str(e)}")
            self._update_task_status(task.id, CollectionStatus.ABORTED)
            raise e
