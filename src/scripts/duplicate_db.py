from pathlib import Path

from databases.db_mgmt import DatabaseManager
from databases.db_models import DBCollectionTask, DBPost
from databases.external import DBConfig, SQliteConnection
from src.const import BASE_DATA_PATH
from sqlalchemy import select

from tools.env_root import root


def dupl(original, model):
    return model(**{
        k: v for k, v in original.__dict__.items()
        if not k.startswith('_')  # Skip SQLAlchemy internal attrs
           and k != 'id'  # Skip primary key
    })


def find_duplicates(db_config: DBConfig, new_db_config: DBConfig):
    db = DatabaseManager(db_config)
    new_db = DatabaseManager(new_db_config)

    with new_db.get_session() as new_session:

        with db.get_session() as session:
            added_tasks: dict[str, DBCollectionTask] = {}

            q = select(DBPost, DBCollectionTask).where(DBPost.collection_task_id == DBCollectionTask.id)
            res = session.execute(q)
            for idx, a in enumerate(res):
                # print(idx)
                new_post = dupl(a[0], DBPost)
                new_session.add(new_post)
                if a[1]:
                    if a[1].task_name in added_tasks:
                        new_post.collection_task = added_tasks[a[1].task_name]
                    else:
                        task = dupl(a[1], DBCollectionTask)
                        new_session.add(task)
                        added_tasks[task.task_name] = task
                        new_post.collection_task = task
                        print(f"adding {task.task_name}")
        print("done")


if __name__ == "__main__":
    root(".")
    existing = DBConfig(db_connection=SQliteConnection(db_path=Path(BASE_DATA_PATH / "youtube.sqlite")))
    new = DBConfig(db_connection=SQliteConnection(db_path=Path((BASE_DATA_PATH / "new.sqlite"))))
    find_duplicates(existing, new)
