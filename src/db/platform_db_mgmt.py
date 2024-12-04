from sqlite3 import IntegrityError

import sqlalchemy
from sqlalchemy import exists

from src.const import BASE_DATA_PATH
from src.db.db_mgmt import DatabaseManager, DatabaseConfig
from src.db.db_models import DBCollectionTask
from src.misc.project_logging import get_b5_logger


class PlatformDB:
    
    __connections: dict[str, "PlatformDB"] = {} 

    @classmethod
    def get_platform_default_db(cls, platform: str) -> DatabaseConfig:
        connection_str = f"sqlite:///{(BASE_DATA_PATH / platform).as_posix()}.sqlite"
        return DatabaseConfig("sqlite", connection_str)


    def __new__(cls, platform: str, *args, **kwargs):
        instance = super().__new__(cls)
        if platform not in cls.__connections:
            cls.__connections[platform] = instance
        return cls.__connections[platform]

    def __init__(self, platform: str):
        self.platform = platform
        self.db_config = self.get_platform_default_db(platform)
        self.db_mgmt = DatabaseManager(self.db_config)
        # todo : make platform specific
        self.logger = get_b5_logger(__file__)

    def check_task_name_exists(self, task_name: str) -> bool:
        with self.db_mgmt.get_session() as session:
            return session.query(exists().where(DBCollectionTask.task_name == task_name)).scalar()

    def add_db_collection_task(self,  collection_task: "ClientTaskConfig") -> bool:
        task_name = collection_task.task_name
        exists_and_overwrite = False
        if self.check_task_name_exists(task_name):
            if collection_task.test and collection_task.overwrite:
                exists_and_overwrite = True
            else:
                self.logger.info(f"client collection task exists already: {task_name}")
                return False
        with self.db_mgmt.get_session() as session:
            task = DBCollectionTask(
                task_name=task_name,
                platform=collection_task.platform,
                collection_config=collection_task.model_dump()["collection_config"],
            )
            if exists_and_overwrite:
                self.logger.debug(f"Collection task set to test and overwrite. overwriting existing task")
                prev = session.query(DBCollectionTask).where(DBCollectionTask.task_name == task_name)
                task.id = task.id
                try:
                    prev.delete()
                except sqlalchemy.exc.IntegrityError as e:
                    session.rollback()  # Rollback changes on error
                    self.logger.error("Failed to delete entry: %s", e)
                    # Handle or re-raise the exception as needed
                    return False

            session.add(task)
            session.commit()
            self.logger.info(f"Added new client collection task: {task_name}")
            return True
