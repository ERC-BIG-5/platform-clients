from sqlalchemy import select

from big5_databases.databases.db_mgmt import DatabaseManager
from big5_databases.databases.db_models import Base, DBPost
from big5_databases.databases.external import DBConfig
from src.const import BASE_DATA_PATH
from tools.env_root import root


def find_duplicates(db_config: DBConfig, col: str, model: Base = DBPost, dry= False):
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
    find_duplicates(DBConfig("sqlite", (BASE_DATA_PATH / "youtube.sqlite").as_posix()), "platform_id", dry=False)