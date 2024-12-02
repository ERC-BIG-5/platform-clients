from contextlib import contextmanager
from typing import Optional

from sqlalchemy import create_engine, Engine, event
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database

from src.const import BASE_DATA_PATH
from src.db.db_models import Base


class DatabaseConfig:
    def __init__(self, db_type: str, connection_string: str, reset_db: bool = False,
                 db_name: Optional[str] = None):
        self.db_type = db_type
        self.connection_string = f"sqlite:///{connection_string}" if db_type == "sqlite" and not connection_string.startswith(
            "sqlite:///") else connection_string
        self.reset_db = reset_db
        # todo: refactor. for postgres...
        self.db_name = db_name

    @classmethod
    def get_main_db_config(cls) -> "DatabaseConfig":
        return DatabaseConfig("sqlite", (BASE_DATA_PATH / "main.sqlite").as_posix())


class DatabaseManager:

    __databases: dict[str, "DatabaseManager"] = {}

    def __new__(cls, *args, **kwargs) -> "DatabaseManager":
        instance = super().__new__(cls)
        assert isinstance(args[0], DatabaseConfig)
        config:DatabaseConfig = args[0]
        existing_db_mgmt = cls.__databases.get(config.connection_string)
        if existing_db_mgmt:
            return existing_db_mgmt
        else:
            cls.__databases[config.connection_string] = instance
            return DatabaseManager(config)

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine = self._create_engine()
        self.Session = sessionmaker(self.engine)

        self.init_database()

        if config.db_type == "sqlite":
            event.listen(self.engine, 'connect', self._sqlite_on_connect)

    def _create_engine(self) -> Engine:
        return create_engine(self.config.connection_string)

    @staticmethod
    def _sqlite_on_connect(dbapi_con, _):
        dbapi_con.execute('pragma foreign_keys=ON')

    def _create_postgres_db(self) -> None:
        if database_exists(self.config.connection_string):
            if self.config.reset_db:
                if input(f"Database {self.config.db_name} exists. Drop it? (y/n): ").lower() == 'y':
                    drop_database(self.config.connection_string)
                else:
                    return
            else:
                return

        create_database(self.config.connection_string)
        Base.metadata.create_all(self.engine)

    def init_database(self) -> None:
        """Initialize database, optionally resetting if configured."""
        if self.config.db_type == "postgres":
            self._create_postgres_db()
        else:  # sqlite
            if self.config.reset_db and database_exists(self.engine.url):
                if input(f"Delete existing database? (y/n): ").lower() == 'y':
                    drop_database(self.engine.url)
                else:
                    return

            if not database_exists(self.engine.url):
                create_database(self.engine.url)
                Base.metadata.create_all(self.engine)

    @contextmanager
    def get_session(self):
        """Provide a transactional scope around operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


class AsyncDatabaseManager(DatabaseManager):
    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        self.async_engine = create_async_engine(config.connection_string)
        self.async_session = async_sessionmaker(self.async_engine)

    async def get_async_session(self) -> AsyncSession:
        return self.async_session()

