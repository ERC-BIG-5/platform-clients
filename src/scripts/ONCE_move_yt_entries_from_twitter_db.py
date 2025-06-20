from datetime import datetime
from typing import Optional, Any, Type

from sqlalchemy import select, BinaryExpression, func

from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_models import Base, DBPost, DBCollectionTask
from big5_databases.databases.external import DBConfig
from src.const import BASE_DATA_PATH

conf = DBConfig("sqlite", (BASE_DATA_PATH / "twitter.sqlite").as_posix())
db = DatabaseManager(conf)

def to_dict(item: Base, orm_cls: Type[Base]) -> dict:
    return {
        column.key: getattr(item, column.key)
        for column in orm_cls.__table__.columns
    }

# todo use misc.helper
def get_posts(platform: str,
              conditions: Optional[BinaryExpression | list[BinaryExpression]] = None
              ) -> list[dict[str,Any]]:
    with db.get_session() as session:
        # Start with a base query
        query = select(DBPost).where(DBPost.platform == platform)


        if conditions is not None:
            if isinstance(conditions, list):
                for condition in conditions:
                    query = query.where(condition)
            else:
                query = query.where(conditions)

        # Execute the query and return the results
        result = session.execute(query)
        return [to_dict(r, DBPost) for r in result.scalars().all()]

#twitter_posts = get_posts("twitter")
yt_posts = get_posts("youtube")

all_collection_tasks = set(p["collection_task_id"] for p in yt_posts)

def get_collection_task(task_ids: list[int]) ->list[dict[str,Any]]:

    with db.get_session() as session:
        # Start with a base query
        query = select(DBCollectionTask).where(DBCollectionTask.id.in_(task_ids))
        result = session.execute(query)
        return [to_dict(r, DBCollectionTask) for r in result.scalars().all()]

collection_tasks = get_collection_task(all_collection_tasks)

kickout_tasks = list(filter(lambda t: t["task_name"].startswith("2022_en_all_months_safeSearch-strict"),collection_tasks))
kickout_task_ids = list(t["id"] for t in kickout_tasks)

kickout_posts = list(filter(lambda p: p["collection_task_id"] in kickout_task_ids, yt_posts))
kickout_posts_ids = [p["id"] for p in kickout_posts]

def delete_posts(delete_ids: list[int]):
    with db.get_session() as session:
        session.query(DBPost).filter(
            DBPost.platform == "youtube",
            DBPost.id.in_(kickout_posts_ids)
        ).delete()
        session.commit()
