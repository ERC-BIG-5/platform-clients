from typing import Optional, Sequence

from sqlalchemy import select, exists

from src.const import CollectionStatus
from src.db.db_models import DBPost, DBCollectionTask
from src.db.db_session import Session
from src.misc.project_logging import get_b5_logger

logger = get_b5_logger(__file__)


def check_task_name_exists(task_name: str) -> bool:
    with Session() as session:
        return session.query(exists().where(DBCollectionTask.task_name==task_name)).scalar()

def get_task(task_name: str, session: Session) -> DBCollectionTask:
    return session.query(DBCollectionTask).where(DBCollectionTask.task_name == task_name)

# todo fix import stuff...
def add_db_collection_task(collection_task: "ClientTaskConfig") -> bool:
    task_name = collection_task.task_name
    exists_and_overwrite = False
    if check_task_name_exists(task_name):
        logger.info(f"client collection task exists already: {task_name}")
        if collection_task.test and collection_task.overwrite:
            exists_and_overwrite = True
        else:
            return False
    with Session() as session:
        task = DBCollectionTask(
            task_name=task_name,
            platform=collection_task.platform,
            collection_config=collection_task.model_dump()["collection_config"],
        )
        if exists_and_overwrite:
            prev = session.query(DBCollectionTask).where(DBCollectionTask.task_name == task_name)
            task.id = task.id
            prev.delete()

        session.add(task)
        session.commit()
        logger.info(f"Added new client collection task: {task_name}")
        return True

def filter_duplicate_post_urls(posts: list[DBPost]) -> list[DBPost]:
    post_urls:set[str] = set()
    accepted_posts: list[DBPost] = []
    for post in posts:
        if post.post_url not in post_urls:
            accepted_posts.append(post)
            post_urls.add(post.post_url)
    return accepted_posts

def filter_posts_with_existing_post_urls(posts: list[DBPost]) -> list[DBPost]:
    post_urls = [p.post_url for p in posts]
    with Session() as session:
        query = select(DBPost.post_url).where(DBPost.post_url.in_(post_urls))
        found_post_urls = session.execute(query).scalars().all()
        logger.debug(f"filtering posts with urls: {found_post_urls}")
    return list(filter(lambda p: p.post_url not in found_post_urls, posts))

def submit_posts(posts: list[DBPost]):
    posts = filter_duplicate_post_urls(posts)
    posts = filter_posts_with_existing_post_urls(posts)
    with Session() as session:
        session.add_all(posts)
        session.commit()


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


def get_task_queue(platforms: Optional[Sequence[str]] = None) -> list[DBCollectionTask]:
    queueable_statuses = [CollectionStatus.INIT, CollectionStatus.ACTIVE, CollectionStatus.PAUSED]

    query = select(DBCollectionTask).where(DBCollectionTask.status.in_(queueable_statuses))
    if platforms is not None:
        query = query.where(DBCollectionTask.platform.in_(list(platforms)))

    with Session() as session:
        result = session.execute(query)
        return result.scalars().all()


def get_task(task_id: int) -> DBCollectionTask:
    with Session() as session:
        return session.scalar(select(DBCollectionTask).where(DBCollectionTask.id == task_id))


def set_task_status(task_id: int, status: CollectionStatus):
    task: DBCollectionTask = get_task(task_id)
    with Session() as session:
        task.status = status
        session.add(task)
        session.commit()
