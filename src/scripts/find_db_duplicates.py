from sqlalchemy import select
from src.const import BASE_DATA_PATH
from src.db.db_mgmt import DatabaseConfig, DatabaseManager
from src.db.db_models import Base, DBPost
from tools.env_root import root


def find_duplicates(db_config: DatabaseConfig, col: str, model: Base = DBPost, dry= False):
    db = DatabaseManager(db_config)

    uniques = set()
    to_remove = []
    with db.get_session() as session:
        items = session.execute(select(model)).scalars()
        for item in items:
            val = getattr(item, col)
            if val in uniques:
                to_remove.append(item)
                if not dry:
                    session.delete(item)
                to_remove.append(item)
            else:
                uniques.add(val)

    if dry:
        print(f"Would remove {len(to_remove)}")
    else:
        print(f"Removed {len(to_remove)}")

if __name__ == "__main__":
    root(".")
    find_duplicates(DatabaseConfig("sqlite", (BASE_DATA_PATH / "youtube.sqlite").as_posix()), "platform_id", dry=False)