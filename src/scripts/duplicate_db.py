from src.const import BASE_DATA_PATH
from src.db.db_mgmt import DatabaseConfig, DatabaseManager
from sqlalchemy import select

from src.db.db_models import DBPost, DBCollectionTask
from tools.env_root import root

def dupl(original, model):
    return model(**{
        k: v for k, v in original.__dict__.items()
        if not k.startswith('_')  # Skip SQLAlchemy internal attrs
           and k != 'id'  # Skip primary key
    })

def find_duplicates(db_config: DatabaseConfig, new_db_config: DatabaseConfig):
    db = DatabaseManager(db_config)
    new_db = DatabaseManager(new_db_config)

    with new_db.get_session() as new_session:

        with db.get_session() as session:
            added_tasks:dict[str, DBCollectionTask] = {}

            q = select(DBPost, DBCollectionTask).where(DBPost.collection_task_id == DBCollectionTask.id)
            res = session.execute(q)
            for idx, a in enumerate(res):
                #print(idx)
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
    existing = DatabaseConfig("sqlite", (BASE_DATA_PATH / "youtube.sqlite").as_posix())
    new = DatabaseConfig("sqlite", (BASE_DATA_PATH / "new.sqlite").as_posix())
    find_duplicates(existing, new)
