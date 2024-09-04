from typing import Optional

from sqlalchemy import select

from src.db.db_models import DBPost
from src.db.db_session import Session


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