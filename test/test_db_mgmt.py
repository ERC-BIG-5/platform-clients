import os
from datetime import datetime

from sqlalchemy import inspect
from sqlalchemy.orm import Session
from src.db.db_mgmt import DatabaseConfig, DatabaseManager
from src.db.db_models import DBPost


def setup_function(function):
    """Setup test database file."""
    if os.path.exists("test.sqlite"):
        os.remove("test.sqlite")

def teardown_function(function):
    """Teardown test database file."""
    if os.path.exists("test.sqlite"):
        os.remove("test.sqlite")

def test_create_engine():
    """Test that the _create_engine method creates an engine."""
    config = DatabaseConfig("sqlite", "test.sqlite")
    db_manager = DatabaseManager(config)

    engine = db_manager._create_engine()

    assert engine is not None
    assert engine.url.database == "test.sqlite"

def test_init_database():
    """Test that init_database creates the tables."""
    config = DatabaseConfig("sqlite", "test.sqlite")
    db_manager = DatabaseManager(config)

    db_manager.init_database()

    inspector = inspect(db_manager.engine)
    # todo test more
    assert "post" in inspector.get_table_names()

def test_get_session():
    """Test that get_session provides a working session."""
    config = DatabaseConfig("sqlite", "test.sqlite")
    db_manager = DatabaseManager(config)
    db_manager.init_database()

    with db_manager.get_session() as session:
        assert session is not None
        assert isinstance(session, Session)

def test_write_and_read_objects():
    """Test writing to and reading from the database."""
    config = DatabaseConfig("sqlite", "test.sqlite")
    db_manager = DatabaseManager(config)
    db_manager.init_database()

    with db_manager.get_session() as session:
        new_entry = DBPost(platform="youtube",platform_id="djksajksjak", date_created=datetime.now())
        session.add(new_entry)


    with db_manager.get_session() as session:
        result = session.query(DBPost).first()
        assert result is not None
        #assert result.name == "Test Name"
