from sys import platform
from typing import Optional, Sequence

from sqlalchemy import select, exists

from src.clients.clients_models import ClientTaskConfig
from src.db.db_models import DBPost, CollectionTask, CollectionStatus
from src.db.db_session import Session
from src.misc.project_logging import get_b5_logger

logger = get_b5_logger(__file__)


def check_task_name_exists(task_name: str) -> bool:
    with Session() as session:
        stmt = exists().where(CollectionTask.task_name == task_name)
        result = session.query(stmt).scalar()
        return bool(result)


def add_db_collection_task(collection_task: ClientTaskConfig) -> bool:
    task_name = collection_task.task_name
    if check_task_name_exists(task_name):
        logger.debug(f"client collection task exists already: {task_name}")
        return False
    with Session() as session:
        task = CollectionTask(
            task_name=task_name,
            platform=collection_task.platform,
            config_data={
                "base_collection_config": collection_task.base_collection_config.model_dump(),
                "collection_steps": [s.model_dump() for s in collection_task.collection_steps],
            },
            total_steps=len(collection_task.collection_steps)
        )
        session.add(task)
        session.commit()
        logger.info(f"Added new client collection task: {task_name}")
        return True


def get_posts(platform: str,
              task_name: Optional[str] = None,
              task_step: Optional[int] = None) -> list[DBPost]:
    # Start with a base query
    query = select(DBPost)

    # Platform is required
    query = query.where(DBPost.platform == platform)

    # If task_name is provided, add it to the query
    if task_name is not None:
        query = query.where(DBPost.collection_task_name == task_name)

    # If task_step is provided, add it to the query
    # Note: We only add this if task_name is also provided
    if task_name is not None and task_step is not None:
        query = query.where(DBPost.collection_step == task_step)

    # Execute the query and return the results
    with Session() as session:
        result = session.execute(query)
        return result.scalars().all()


def get_task_queue(platforms: Optional[Sequence[str]] = None) -> list[CollectionTask]:
    queueable_statuses = [CollectionStatus.INIT,CollectionStatus.ACTIVE]

    query = select(CollectionTask).where(CollectionTask.status.in_(queueable_statuses))
    if platforms is not None:
        query = query.where(CollectionTask.platform.in_(list(platforms)))


    with Session() as session:
        result = session.execute(query)
        return result.scalars().all()