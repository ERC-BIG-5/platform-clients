from contextlib import contextmanager
from typing import Optional

from sqlalchemy import create_engine, Engine, event
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database, drop_database


class DatabaseConfig:
    def __init__(self, db_type: str, connection_string: str, base_model, reset_db: bool = False,
                 db_name: Optional[str] = None):
        self.db_type = db_type
        self.connection_string = f"sqlite:///{connection_string}" if db_type == "sqlite" and not connection_string.startswith(
            "sqlite:///") else connection_string
        self.base_model = base_model
        self.reset_db = reset_db
        self.db_name = db_name


class DatabaseManager:
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine = self._create_engine()
        self.Session = sessionmaker(self.engine)

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
        self.config.base_model.metadata.create_all(self.engine)

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
                self.config.base_model.metadata.create_all(self.engine)

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


# Usage example:
"""
# PostgreSQL Configuration
postgres_config = DatabaseConfig(
    db_type="postgres",
    connection_string="postgresql://user:pass@localhost:5432/mydb",
    base_model=Base,
    reset_db=True,
    db_name="mydb"
)

# SQLite Configuration
sqlite_config = DatabaseConfig(
    db_type="sqlite",
    connection_string=f"sqlite:///{Path('data/db.sqlite')}",
    base_model=Base,
    reset_db=False
)

# Create manager instance
db = DatabaseManager(postgres_config)  # or sqlite_config
db.init_database()

# Use in synchronous context
with db.get_session() as session:
    user = User(name="John")
    session.add(user)
"""