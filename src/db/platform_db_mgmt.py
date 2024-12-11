import sqlalchemy
from sqlalchemy import exists

from src.clients.clients_models import DBConfig, SQliteConnection
from src.const import BASE_DATA_PATH
from src.db.db_mgmt import DatabaseManager, DatabaseConfig
from src.db.db_models import DBCollectionTask, DBPost
from tools.project_logging import get_logger


class PlatformDB:
    """
    Singleton class to manage platform-specific database connections
    """
    __connections: dict[str, "PlatformDB"] = {}

    @classmethod
    def get_platform_default_db(cls, platform: str) -> DBConfig:
        return DBConfig(db_connection=SQliteConnection(
            db_path=(BASE_DATA_PATH / f"{platform}.sqlite").as_posix()
        ))

    def __new__(cls, platform: str, *args, **kwargs):
        instance = super().__new__(cls)
        if platform not in cls.__connections:
            cls.__connections[platform] = instance
        return cls.__connections[platform]

    def __init__(self, platform: str, db_config: DBConfig = None):
        # Only initialize if this is a new instance
        if not hasattr(self, 'initialized'):
            self.platform = platform
            self.db_config = db_config or self.get_platform_default_db(platform)
            self.db_mgmt = DatabaseManager(self.db_config)
            self.logger = get_logger(__file__)
            self.initialized = True

    def check_task_name_exists(self, task_name: str) -> bool:
        with self.db_mgmt.get_session() as session:
            return session.query(exists().where(DBCollectionTask.task_name == task_name)).scalar()

    def add_db_collection_task(self, collection_task: "ClientTaskConfig") -> bool:
        task_name = collection_task.task_name
        exists_and_overwrite = False
        if self.check_task_name_exists(task_name):
            if collection_task.test and collection_task.overwrite:
                exists_and_overwrite = True
            else:
                self.logger.info(f"client collection task exists already: {task_name}")
                return False
        with self.db_mgmt.get_session() as session:
            # specific function. refactor out
            task = DBCollectionTask(
                task_name=task_name,
                platform=collection_task.platform,
                collection_config=collection_task.model_dump()["collection_config"],
                transient=collection_task.transient,
            )
            if exists_and_overwrite:
                self.logger.debug(f"Collection task set to test and overwrite. overwriting existing task")
                prev = session.query(DBCollectionTask).where(DBCollectionTask.task_name == task_name)
                task.id = task.id
                try:
                    session.query(DBPost).where(DBPost.collection_task_id == prev.first().id).delete(
                        synchronize_session=False
                    )
                    prev.delete(synchronize_session=False)
                except sqlalchemy.exc.IntegrityError as e:
                    session.rollback()  # Rollback changes on error
                    self.logger.warning(f"Failed to delete exising task: {task.task_name} ({repr(e)}")
                    # Handle or re-raise the exception as needed
                    return False

            session.add(task)
            session.commit()
            self.logger.info(f"Added new client collection task: {task_name}")
            return True


    def get_db_manager(self) -> DatabaseManager:
        """Get the underlying database manager"""
        return self.db_mgmt
