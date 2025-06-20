from typing import Iterable

import sqlalchemy
from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_models import DBPost
from big5_databases.databases.external import DBConfig, BASE_DATA_PATH, SQliteConnection
from sqlalchemy.orm.attributes import flag_modified

def main():
    metadata_filled_db_p = BASE_DATA_PATH / "twitter_20_01_2025_backup.sqlite"
    full_db_p = BASE_DATA_PATH / "twitter.sqlite"

    md_db = DatabaseManager(DBConfig(db_connection=SQliteConnection(db_path=metadata_filled_db_p)))

    full_db = DatabaseManager(DBConfig(db_connection=SQliteConnection(db_path=full_db_p)))

    batch_size = 50

    with (md_db.get_session() as session):
        query = sqlalchemy.select(DBPost)
        query = query.order_by(DBPost.date_created)
        res: Iterable[DBPost] = session.execute(query).scalars()

        # platform_id: metadata
        batch: dict[str, dict] = {}
        for post in res:
            # print(post.id)
            batch[post.platform_id] = post.metadata_content or {}

            if len(batch) == batch_size:
                with full_db.get_session() as main_session:
                    main_batch_posts = main_session.execute(sqlalchemy.select(DBPost
                                                                              ).where(
                        DBPost.platform_id.in_(list(batch)))).scalars().all()

                    main_batch_posts_dict = {post.platform_id: post for post in main_batch_posts}

                    if len(main_batch_posts_dict) != batch_size:
                        print("problems with getting all posts...")
                    print("updating", [post.id for post in main_batch_posts])
                    for p_id, p in main_batch_posts_dict.items():
                        p.metadata_content = batch.get(p_id)
                        flag_modified(p, "metadata_content")
                batch.clear()

if __name__ == "__main__":
    print("this is done...")
    # main()
