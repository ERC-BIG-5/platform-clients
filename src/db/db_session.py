from pathlib import Path
from typing import Literal

from sqlalchemy import create_engine, Engine, event
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database

from src.const import BASE_DATA_PATH, BIG5_CONFIG
from src.db.db_models import Base
from src.db.db_settings import PostgresCredentials, SqliteSettings
from src.misc.files import relative_to_project_path
from src.misc.project_logging import get_b5_logger

logger = get_b5_logger(__file__)

def create_postgres_db(db_name: str, drop_existing: bool = False) -> None:
    """
    Create the database and tables based on the defined models.

    """
    conn_str = PostgresCredentials(DB_NAME=db_name).connection_str
    create = False
    if database_exists(conn_str):
        if drop_existing:
            answer = input(f"Database {db_name} already exists. Do you want to drop it? (y/n)")
            if answer == "y":
                drop_database(conn_str)
                create = True
            else:
                print("Aborting")
                return
    else:
        create = True
    if create:
        create_database(conn_str)

    engine = create_engine(conn_str)
    Base.metadata.create_all(engine)

def sqlite_db_path() -> Path:
    return (BASE_DATA_PATH / SqliteSettings().SQLITE_FILE_PATH).absolute()

def sqlite_engine() -> Engine:
    return  create_engine(f'sqlite:///{sqlite_db_path().as_posix()}')

def create_sqlite_db():
    """
    Create the SQLite database and tables based on the defined models.
    """
    logger.debug("create_sqlite_db")
    engine = sqlite_engine()

    def _fk_pragma_on_connect(dbapi_con, con_record):
        dbapi_con.execute('pragma foreign_keys=ON')
    # Enable foreign key support for SQLite
    event.listen(engine, 'connect', _fk_pragma_on_connect)

    # Create the database if it doesn't exist
    if not database_exists(engine.url):
        create_database(engine.url)
        # TODO test, which of those is needed?
        # Create all tables in the database
        Base.metadata.create_all(engine)
    # print(f"SQLite database created at {db_path}")



if BIG5_CONFIG.db_type == "postgres":
    # normal and async session_maker
    engine = create_engine(PostgresCredentials().connection_str)
    session = sessionmaker(engine)

    async_engine = create_async_engine(PostgresCredentials().connection_str)
    """
    async sessions...
    e.g.
    
    >>> async def insert_objects(async_session: async_sessionmaker[AsyncSession]) -> None:
    ...     async with async_session() as session:
    ...         async with session.begin():
    ...             session.add_all(
    ...                 [
    ...                     A(bs=[B(data="b1"), B(data="b2")], data="a1"),
    ...                     A(bs=[], data="a2"),
    ...                     A(bs=[B(data="b3"), B(data="b4")], data="a3"),
    ...                 ]
    ...             )
    """
    async_session: async_sessionmaker[AsyncSession] = async_sessionmaker()
else:
    engine = sqlite_engine()
    Session = sessionmaker(engine)

def init_db():
    if BIG5_CONFIG.db_type == "sqlite":
        if BIG5_CONFIG.reset_db:
            delete_resp = input(f"Do you want to delete the db? : y/ other key\n")
            if delete_resp == "y":
                logger.info(f"deleting: {relative_to_project_path(sqlite_db_path())}")
                sqlite_db_path().unlink()

        create_sqlite_db()
    # ...
    elif BIG5_CONFIG.db_type == "postgres":
        create_postgres_db("big5", True)

if __name__ == "__main__":
    init_db()
    Session = sessionmaker(bind=engine)
    session = Session()

    # Create a user


    # check(session.connection())
    # Session = sessionmaker(engine)
    # with Session() as session:
    #     print(list(session.query(Post)))
    # Create a new user
    # new_user = User(name="John Doe", email="john@example.com")
    # session.add(new_user)
    # session.commit()
    # async_sessionmaker():

    # Query the user
    # user = session.query(User).filter_by(name="John Doe").first()
    # print(user)

    # session.close()
