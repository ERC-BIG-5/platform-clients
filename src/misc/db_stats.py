import json
import os
import shutil
from collections import namedtuple, Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Generator

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from orjson import orjson
from sqlalchemy import select, BinaryExpression

from databases.db_mgmt import DatabaseManager
from databases.db_models import DBPost, DBCollectionTask
from databases.db_utils import filter_posts_with_existing_post_ids
from databases.external import DBConfig, SQliteConnection
from databases.model_conversion import PostModel
from src.const import BASE_DATA_PATH

stats_copy_path = BASE_DATA_PATH / "stats_copy.sqlite"
from dataclasses import field
from datetime import date
from datetime import datetime
import matplotlib.dates as mdates
from dataclasses import asdict

RAISE_DB_ERROR = True


def make_stats_copy(db_path: Path):
    shutil.copy(db_path, stats_copy_path)


def delete_stats_copy():
    if os.path.exists(stats_copy_path):
        os.remove(stats_copy_path)




@dataclass
class PlatformStats:
    name: str
    post_count: int = 0
    min_date: date = field(default_factory=datetime.max.date)
    max_date: date = field(default_factory=datetime.min.date)
    year_month_count: Counter[str, int] | dict[str,int] = field(default_factory=Counter)
    last_collected: date = field(default_factory=datetime.min.date)

    def add_post(self, post: DBPost):
        self.post_count += 1
        created = post.date_created.date()
        self.min_date = min(self.min_date, created)
        self.max_date = max(self.max_date, created)
        self.year_month_count[f"{created:%Y_%m}"] += 1
        self.last_collected = max(self.last_collected, post.date_collected.date())

    def make_serializable(self) -> dict:
        self.year_month_count = dict(self.year_month_count)

@dataclass
class DBStats:
    db_path: Path | str
    platforms: dict[str, PlatformStats] = field(default_factory=dict)
    error: Optional[str] = None

    def add_post(self, post: DBPost | PostModel):
        self.platforms.setdefault(post.platform, PlatformStats(post.platform)).add_post(post)

    def serializable_dict(self) -> dict:
        self.db_path = self.simple_path
        for pd in self.platforms.values():
            pd.make_serializable()
        return json.loads(orjson.dumps(asdict(self)))

    @property
    def simple_path(self) -> str:
        return self.db_path.relative_to(BASE_DATA_PATH).as_posix()


# todo use misc.helper
def get_posts(db: DatabaseManager,
              conditions: Optional[BinaryExpression | list[BinaryExpression]] = None
              ) -> Generator[PostModel, None, None]:
    with db.get_session() as session:
        query = select(DBPost)
        if conditions is not None:
            if isinstance(conditions, list):
                for condition in conditions:
                    query = query.where(condition)
            else:
                query = query.where(conditions)

        # Execute the query and return the results
        result = session.execute(query).scalars()
        for post in result:
            yield post.model()


# def get_year_counts(year: int) -> pd.Series:
#     results = get_posts("twitter",
#                         conditions=DBPost.date_created.between(
#                             datetime(year, 1, 1),
#                             datetime(year + 1, 1, 1)
#                         ))
#
#     # Convert to pandas Series with days as index
#     counts = pd.Series({row[0]: row[1] for row in results})
#     return counts


# def get_year_month_counts(year: int, month: int) -> pd.Series:
#     results = get_posts("twitter",
#                         conditions=DBPost.date_created.between(
#                             datetime(year, month, 1),
#                             datetime(year, month + 1, 1)
#                         ))
#
#     # Convert to pandas Series with days as index
#     counts = pd.Series({row[0]: row[1] for row in results})
#     return counts


def plot_daily_items(daily_counts: pd.Series, bars: bool = False):
    plt.figure(figsize=(12, 6))

    # Convert index to datetime if not already
    if not isinstance(daily_counts.index, pd.DatetimeIndex):
        daily_counts.index = pd.to_datetime(daily_counts.index)

    if bars:
        plt.bar(daily_counts.index, daily_counts.values,
                color='blue', label='Posts', alpha=0.7)
    else:
        sns.lineplot(data=daily_counts, color='blue', label='Posts')

    # Zero days highlight in red
    zero_days = daily_counts[daily_counts == 0]
    if not zero_days.empty:
        if bars:
            plt.bar(zero_days.index, zero_days.values,
                    color='red', label='No Posts',
                    zorder=5)
        else:
            plt.scatter(zero_days.index, zero_days.values,
                        color='red', s=10, label='No Posts',
                        zorder=5)

    plt.title('Daily Post Count (Red Bars = No Posts)')
    plt.xlabel('Date')
    plt.ylabel('Number of Posts')

    # Improved x-axis labels
    plt.gca().xaxis.set_major_locator(mdates.MonthLocator())
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    plt.xticks(rotation=45)

    plt.grid(True, alpha=0.3)
    # plt.legend()
    plt.tight_layout()
    return plt


class DBMerger:
    BATCH_SIZE = 100

    def __init__(self, db_path: Path, platform: str, add_fake_collection_task: bool = True):
        self.db_path = db_path
        self.db = DatabaseManager(DBConfig(db_connection=SQliteConnection(db_path=db_path)))
        self.batch: list[PostModel] = []
        self.platforms = platform
        if add_fake_collection_task:
            self.add_fake_collection_task()
        else:
            raise NotImplementedError("add_fake_collection_task is not implemented.")

    def add_fake_collection_task(self):
        with self.db.get_session() as session:
            session.add(DBCollectionTask(task_name="fake_collection", platform=self.platforms, collection_config={}))

    def add_post(self, post: PostModel, orig_db_name: str):
        self.batch.append(post)

        if len(self.batch) >= self.BATCH_SIZE:
            posts: list[PostModel] = filter_posts_with_existing_post_ids(self.batch, self.db)
            db_posts: list[DBPost] = []
            for post in posts:
                md = post.metadata_content
                md.orig_db_conf = (orig_db_name, post.collection_task_id)
                post.collection_task_id = 1
                post_d = post.model_dump(exclude={"id"})
                db_posts.append(DBPost(**post_d))

            self.db.submit_posts(db_posts)
            self.batch.clear()


def process_db(db_path: Path, merger_dbs: Optional[dict[str, DBMerger]] = None) -> DBStats:
    make_stats_copy(db_path)
    stats = DBStats(db_path=db_path)

    try:
        db = DatabaseManager(DBConfig(db_connection=SQliteConnection(db_path=stats_copy_path)))
        for post in get_posts(db):
            stats.add_post(post)
            if merger_dbs:
                if post.platform in merger_dbs:
                    merger_dbs[post.platform].add_post(post, stats.simple_path)
    except Exception as e:
        if RAISE_DB_ERROR:
            raise e
        print(e)
        stats.error = str(e)
    finally:
        delete_stats_copy()

    return stats


def create_merge_db():
    dest_file = BASE_DATA_PATH / f"stats/{datetime.now():%Y%m%-d%H}.json"
    all_stats_data: list[dict] = []

    skip = ["db.sqlite", "twitter_merged.sqlite", "youtube_merged.sqlite"]

    merger_dbs: dict[str, DBMerger] = {}
    for platform in ["twitter", "youtube"]:
        os.remove(BASE_DATA_PATH / f"{platform}_merged.sqlite")
        merger_dbs[platform] = DBMerger(BASE_DATA_PATH / f"{platform}_merged.sqlite", platform)

        all_dbs = BASE_DATA_PATH.glob("*.sqlite")

        for db_path in all_dbs:
            print(db_path)
            if db_path.name in skip:
                continue
            stats = process_db(db_path, merger_dbs)
            all_stats_data.append(stats.serializable_dict())

        json.dump(all_stats_data, dest_file.open("w"), indent=4)


def inspect_mergers():
    dest_file = BASE_DATA_PATH / f"stats/{datetime.now():%Y%m%-d%H_M}.json"
    all_stats_data: list[dict] = []

    all_dbs = [BASE_DATA_PATH / f"{platform}_merged.sqlite" for platform in ["twitter", "youtube"]]

    for db_path in all_dbs:
        print(db_path)
        stats = process_db(db_path)
        all_stats_data.append(stats.serializable_dict())

    json.dump(all_stats_data, dest_file.open("w"), indent=4)


if __name__ == "__main__":
    # create_merge_db()
    inspect_mergers()
