from pathlib import Path
from typing import Optional, Sequence, TypedDict

from sqlalchemy import select, exists

from src.const import CollectionStatus, BASE_DATA_PATH
from src.db.db_mgmt import DatabaseManager, DatabaseConfig
from src.db.db_models import DBPost, DBCollectionTask, Base, DBPlatformDatabase, db_m2dict, M_DBPlatformDatabase
from src.db.platform_db_mgmt import PlatformDB
from src.misc.project_logging import get_b5_logger

logger = get_b5_logger(__file__)


def get_task(task_name: str) -> DBCollectionTask:
    return session.query(DBCollectionTask).where(DBCollectionTask.task_name == task_name)


def filter_duplicate_post_urls(posts: list[DBPost]) -> list[DBPost]:
    post_urls: set[str] = set()
    accepted_posts: list[DBPost] = []
    for post in posts:
        if post.post_url not in post_urls:
            accepted_posts.append(post)
            post_urls.add(post.post_url)
    return accepted_posts


def filter_posts_with_existing_post_urls(posts: list[DBPost],db_mgmt: DatabaseManager) -> list[DBPost]:
    post_urls = [p.post_url for p in posts]
    with db_mgmt.get_session() as session:
        query = select(DBPost.post_url).where(DBPost.post_url.in_(post_urls))
        found_post_urls = session.execute(query).scalars().all()
        logger.debug(f"filtering posts with urls: {found_post_urls}")
    return list(filter(lambda p: p.post_url not in found_post_urls, posts))


def submit_posts(posts: list[DBPost], db_mgmt: DatabaseManager) -> int:
    posts = filter_duplicate_post_urls(posts)
    posts = filter_posts_with_existing_post_urls(posts, db_mgmt)
    logger.debug(f"After filtering duplicates... submitting {len(posts)} posts")

    if posts:
        with db_mgmt.get_session() as session:
            session.add_all(posts)
            session.commit()
    return len(posts)


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

    platforms_d_models = main_db_get_all_platforms()
    if platforms:
        platforms_d_models = list(filter(lambda p: p in platforms, platforms_d_models))
    for platform in platforms_d_models:
        platform_db_mgmt = PlatformDB(platform["platform"])
        query = select(DBCollectionTask).where(DBCollectionTask.status.in_(queueable_statuses))
        if platforms is not None:
            query = query.where(DBCollectionTask.platform.in_(list(platforms)))

        # TODO, ended here. sessions will be over. but we need to merge all tasks
        # and return a different model
        with platform_db_mgmt.db_mgmt.get_session() as session:
            result = session.execute(query)
            return result.scalars().all()


def get_task(task_id: int) -> DBCollectionTask:
    with Session() as session:
        return session.scalar(select(DBCollectionTask).where(DBCollectionTask.id == task_id))


def task_done(task: "ClientTaskConfig"):
    with Session() as session:
        db_obj = get_task(task.id)
        session.add(db_obj)
        session.commit()


def set_task_status(task_id: int, status: CollectionStatus,
                    found_items: Optional[int] = None, added_items: Optional[int] = None,
                    duration: Optional[int] = None):
    task: DBCollectionTask = get_task(task_id)
    with Session() as session:
        task.status = status
        if found_items != None:
            task.found_items = found_items
        if added_items != None:
            task.added_items = added_items
        if duration != None:
            task.collection_duration = int(duration * 1000)
        session.add(task)
        session.commit()


def main_db_add_new_db(platform: str, connection_str: str):
    main_db_mgmt = DatabaseManager(DatabaseConfig.get_main_db_config())
    with main_db_mgmt.get_session() as session:
        if session.query(exists().where(DBPlatformDatabase.platform == platform)).scalar():
            return
        session.add(DBPlatformDatabase(platform=platform, connection_str=connection_str))
        session.commit()


def main_db_get_all_platforms() -> list[M_DBPlatformDatabase]:
    main_db_mgmt = DatabaseManager(DatabaseConfig.get_main_db_config())
    with main_db_mgmt.get_session() as session:
        return [
            db_m2dict(m) for m in
            session.execute(select(DBPlatformDatabase)).scalars().all()]
