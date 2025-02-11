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


    # async def execute_task(self, task: ClientTaskConfig) -> list[DBPost]:
    #     """
    #     Execute YouTube collection task with specific handling for:
    #     - Quota management
    #     - Video metadata collection
    #     - Comment collection (if configured)
    #     """
    #     try:
    #         self._update_task_status(task.id, CollectionStatus.RUNNING)
    #         start_time = datetime.now()
    #
    #         # YouTube-specific config transformation
    #         yt_config = self.client.transform_config(task.collection_config)
    #
    #         # Execute collection with quota awareness
    #         collected_items = await self.client.collect(
    #             yt_config,
    #             task.collection_config
    #         )
    #
    #         # Process results and create post entries
    #         posts: list[DBPost] = []
    #         users: list[DBUser] = set()  # Use set to avoid duplicate channels
    #
    #         for item in collected_items:
    #             # Create post entry (video)
    #             post = self.client.create_post_entry(item, task)
    #             posts.append(post)
    #
    #             # Create user entry (channel)
    #             if 'channel_data' in item:
    #                 user = self.client.create_user_entry(item['channel_data'])
    #                 users.add(user)
    #
    #             # Insert data into database
    #             self.insert_users(users)
    #
    #         num_posts_added = self.insert_posts(posts)
    #
    #         # Update task status and statistics
    #         duration = (datetime.now() - start_time).total_seconds()
    #         with self.db_mgmt.get_session() as session:
    #             task_record = session.query(DBCollectionTask).get(task.id)
    #             task_record.status = CollectionStatus.DONE
    #             task_record.found_items = len(collected_items)
    #             task_record.added_items = num_posts_added
    #             task_record.collection_duration = int(duration * 1000)
    #             session.commit()
    #
    #         # Handle rate limiting
    #         if self.client.request_delay:
    #             await sleep(self.client.config.request_delay)
    #
    #         self.logger.info(f"Task '{task.task_name}' completed: {num_posts_added}/{len(collected_items)} posts added")
    #         return posts
    #
    #     except Exception as e:
    #         self.logger.error(f"Error executing YouTube task {task.task_name}: {str(e)}")
    #         self._update_task_status(task.id, CollectionStatus.ABORTED)
    #         raise e




