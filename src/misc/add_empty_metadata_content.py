from pathlib import Path

from tqdm import tqdm

from databases.db_mgmt import DatabaseManager
from databases.db_models import DBPost
from databases.external import DBConfig, SQliteConnection




if __name__ == "__main__":
    db_path = "/home/rsoleyma/projects/platforms-clients/data/twitter_20_01_2025_backup.sqlite"
    db = DatabaseManager(DBConfig(db_connection=SQliteConnection(db_path=Path(db_path))))

    with db.get_session() as session:
        for post in tqdm(session.query(DBPost).all()):
            mc = post.metadata_content
            if mc is None:
                post.metadata_content = {}
        session.commit()


