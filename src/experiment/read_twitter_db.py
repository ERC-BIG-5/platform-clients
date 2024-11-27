from src.db.db_models import DBPost
from src.db.db_session import init_db
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

if __name__ == "__main__":
        init_db()

        engine = create_engine("sqlite:////home/rsoleyma/projects/platforms-clients/data/twitter.sqlite")
        Session = sessionmaker(bind=engine)
        with Session() as session:
            for p in session.query(DBPost).limit(10):
                print(p.date_created)
            pass