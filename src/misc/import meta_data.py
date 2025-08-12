from csv import DictReader
from datetime import datetime, date
from operator import itemgetter
from pathlib import Path
from random import choice, choices
from typing import Generator

from sqlalchemy import func, text
from sqlalchemy.orm.attributes import flag_modified
from tqdm.auto import tqdm

from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_models import DBCollectionTask, DBPost
from big5_databases.databases.external import PostType
from big5_databases.databases.model_conversion import PostModel
from src.const import BIG5_CONFIG


def import_meta_files(database: Path, files: list[Path], query: str, platform: str = "instagram", language: str = "en"):
    db = DatabaseManager.sqlite_db_from_path(database, create=True)
    with db.get_session() as session:
        for file in files:

            existing = session.query(DBCollectionTask).where(DBCollectionTask.task_name == file.stem).one_or_none()
            if existing:
                continue

            min_day = None
            max_day = None
            task = DBCollectionTask(task_name=file.stem,
                                    platform=platform,
                                    collection_config={
                                        "query": query,
                                        "from_time": "",
                                        "to_time": "",
                                        "language": language
                                    },
                                    execution_ts=datetime.now())
            session.add(task)

            reader = DictReader(file.open(encoding="utf-8-sig"),
                                fieldnames=['content_type', 'creation_time', 'hashtags', 'id', 'is_branded_content',
                                            'lang', 'match_type', 'mcl_url', 'modified_time', 'multimedia',
                                            'post_owner.type', 'post_owner.id', 'post_owner.name',
                                            'post_owner.username', 'statistics.comment_count', 'statistics.like_count',
                                            'statistics.views', 'statistics.views_date_last_refreshed', 'text'])

            for row in tqdm(reader):
                if row["content_type"] == "content_type":
                    continue
                post_dt = datetime.fromisoformat(row["creation_time"])
                post_d = post_dt.date()
                if not min_day:
                    min_day = post_d
                    max_day = post_d
                else:
                    min_day = min(min_day, post_d)
                    max_day = max(max_day, post_d)

                post = DBPost(
                    platform=platform,
                    platform_id=row["id"],
                    date_created=post_dt,
                    content=row,
                    post_url="",
                    post_type=PostType.REGULAR,
                    collection_task=task)
                session.add(post)

            task.collection_config["from_time"] = min_day.isoformat()
            task.collection_config["to_time"] = max_day.isoformat()
        session.commit()


def sample_from_data(database: Path, label: str, sample_size: int = 1750):
    db = DatabaseManager.sqlite_db_from_path(database)
    from big5_databases.commands import posts_per_period
    day_counts = posts_per_period("instagram-1", "day", False)
    posts_per_day = sample_size // len(day_counts)
    print(f"{posts_per_day=}, {posts_per_day * len(day_counts)=}")
    extra = sample_size - (posts_per_day * len(day_counts))
    print(f"{extra=}")
    selected_posts_ids = []
    for day,num_posts in tqdm(day_counts):
        # print(selection_indices)
        with db.get_session() as session:
            posts = session.query(DBPost).where(func.date(DBPost.date_created) == date.fromisoformat(day)).all()
            selection_indices = sorted(choices(range(len(posts)), k=posts_per_day))
            # print(f"{len(posts)=},{len(posts) == num_posts} {selection_indices[-1]=}")

            selected_posts = itemgetter(*selection_indices)(posts)
            # print(len(selected_posts))
            if len(selected_posts) != len(selection_indices):
                print(f"missing posts for {day}. {len(selection_indices) - len(selected_posts)}")
            for post in selected_posts:
                pm = post.model()
                labels = pm.metadata_content.labels or []
                if label not in labels:
                    labels.append(label)
                    pm.metadata_content.labels = labels
                    post.metadata_content = pm.metadata_content.model_dump(exclude_none=True)
                    flag_modified(post, "metadata_content")
                    session.add(post)
                    selected_posts_ids.append(post.platform_id)
            session.commit()
    return selected_posts_ids


# todo somewhere meta
def remove_label(database: Path, label: str):
    db = DatabaseManager.sqlite_db_from_path(database)
    with db.get_session() as session:
        ll = session.query(DBPost).filter(text(f"'{label}' IN (SELECT value FROM json_each(json_extract(metadata_content, '$.labels')))"))
        for post in tqdm(ll.all()):
            labels = post.metadata_content["labels"] or []
            if labels:
                if label in labels:
                    labels.remove(label)
                    post.metadata_content["labels"] = labels
                    flag_modified(post, "metadata_content")


def get_posts_with_label(database: Path, label: str) -> Generator[PostModel, None, None]:
    db = DatabaseManager.sqlite_db_from_path(database)
    with db.get_session() as session:
        q = session.query(DBPost).filter(text(f"'{label}' IN (SELECT value FROM json_each(json_extract(metadata_content, '$.labels')))"))
        for post in q:
            yield post.model()

if __name__ == "__main__":
    db = Path(BIG5_CONFIG.global_data_folder) / "databases/instagram.sqlite"

    remove_label(db, "test-1")
    p_ids = sample_from_data(db, "test-1")
    print(len(p_ids))
    missing = 0
    for p in get_posts_with_label(db, "test-1"):
        if not p.platform_id in p_ids:
            missing += 1
    print(f"{missing=}")

    print(len(list(get_posts_with_label(db, "test-1"))))