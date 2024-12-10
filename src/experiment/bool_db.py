from src.db.db_models import Base
from sqlalchemy import Boolean, create_engine
from sqlalchemy_utils import database_exists, create_database, drop_database
from sqlalchemy.orm import relationship, Mapped, mapped_column, declarative_base

class Rand(Base):
    __tablename__ = 'test'
    id: Mapped[int] = mapped_column(primary_key=True)
    some:Mapped[bool] = mapped_column(Boolean)

engine = create_engine(f'sqlite:///dest.sqlite')
create_database(engine.url)
# TODO test, which of those is needed?
# Create all tables in the database


Base.metadata.create_all(engine)