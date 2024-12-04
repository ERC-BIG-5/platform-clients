from datetime import datetime
from typing import Type, TypedDict

from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, JSON, Enum, func, UniqueConstraint
from sqlalchemy import Enum as SQLAlchemyEnum
from sqlalchemy.orm import relationship, Mapped, mapped_column, declarative_base

from src.clients.clients_models import CollectionStatus
from src.const import PostType
from src.db.model_conversion import PlatformDatabaseModel, CollectionTaskModel, PostModel

Base = declarative_base()


class DBUser(Base):
    __tablename__ = 'user'

    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    platform_username: Mapped[str] = mapped_column(String(20), nullable=True)

    # posts: Mapped[list["DBPost"]] = relationship(back_populates="user")


# class DBPostContentSchema(Base):
#     __tablename__ = 'post_content_schema'
#
#     id: Mapped[int] = mapped_column(primary_key=True)
#     platform: Mapped[str] = mapped_column(String(20), nullable=False)
#     schema: Mapped[dict] = mapped_column(JSON, nullable=False)
#     posts: Mapped[list["DBPost"]] = relationship(back_populates="content_schema")


class DBComment(Base):
    """
    """
    __tablename__ = 'comment'
    id: Mapped[int] = mapped_column(primary_key=True)
    date_created: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    content: Mapped[str] = mapped_column(String(200), nullable=False)
    date_collected: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    post_id: Mapped[int] = mapped_column(ForeignKey("post.id"))
    post: Mapped["DBPost"] = relationship(back_populates="comments")


class DBCollectionTask(Base):
    __tablename__ = 'collection_task'

    # this for alembic
    __table_args__ = (
        UniqueConstraint('task_name', name='uq_task_name'),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    task_name: Mapped[str] = mapped_column(String(50), nullable=False)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    collection_config: Mapped[dict] = mapped_column(JSON, nullable=False)
    found_items: Mapped[int] = mapped_column(Integer, nullable=True)
    added_items: Mapped[int] = mapped_column(Integer, nullable=True)
    # in millis
    collection_duration: Mapped[int] = mapped_column(Integer, nullable=True)
    status: Mapped[CollectionStatus] = mapped_column(SQLAlchemyEnum(CollectionStatus), nullable=False,
                                                     default=CollectionStatus.INIT)
    time_added: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())
    database: Mapped[str] = mapped_column(String(20), nullable=True)

    def __repr__(self) -> str:
        return f"CollectionTask: '{self.task_name}' / {self.platform}. ({self.status.name})"

    def model(self) -> CollectionTaskModel:
        return CollectionTaskModel.from_orm(self)


class DBPost(Base):
    __tablename__ = 'post'

    # this for alembic
    __table_args__ = (
        UniqueConstraint('post_url', name='uq_post_url'),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    platform_id: Mapped[str] = mapped_column(String(50), nullable=True, unique=False)
    post_url: Mapped[str] = mapped_column(String(60), nullable=False)
    date_created: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    post_type: Mapped[PostType] = mapped_column(Enum(PostType), nullable=False, default=PostType.REGULAR)
    content: Mapped[dict] = Column(JSON)
    date_collected: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())

    # todo: temp nullable
    collection_task_id: Mapped[int] = mapped_column(ForeignKey("collection_task.id"), nullable=True)
    collection_task: Mapped["DBCollectionTask"] = relationship(backref="posts")

    # collection_step: Mapped[int] = mapped_column(Integer, nullable=True)

    # user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    # user: Mapped[DBUser] = relationship(back_populates="posts")

    # content_schema_id: Mapped[int] = mapped_column(ForeignKey("post_content_schema.id"), nullable=True)
    # content_schema: Mapped[DBPostContentSchema] = relationship(back_populates="posts")

    comments: Mapped[list[DBComment]] = relationship(back_populates="post")

    def model(self) -> PostModel:
        return PostModel.from_orm(self)


class DBPlatformDatabase(Base):
    __tablename__ = 'platform_databases'

    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False, unique=True)
    connection_str: Mapped[str] = mapped_column(String(), nullable=False)

    def model(self) -> PlatformDatabaseModel:
        return PlatformDatabaseModel.from_orm(self)


M_DBPlatformDatabase = TypedDict("M_DBPlatformDatabase",
                                 {
                                     "id": int,
                                     "platform": str,
                                     "connection_str": str
                                 })


def db_m2dict(item: Base) -> dict:
    return {
        column.key: getattr(item, column.key)
        for column in type(item).__table__.columns
    }
