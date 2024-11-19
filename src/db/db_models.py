from datetime import datetime

from enum import Enum as PyEnum
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, JSON, Enum, func,UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Mapped, mapped_column

from src.clients.clients_models import CollectionStatus

from src.const import PostType

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

    __table_args__ = (
        UniqueConstraint('task_name', name='uq_task_name'),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    task_name: Mapped[str] = mapped_column(String(50),nullable=False)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    collection_config: Mapped[dict] = mapped_column(JSON, nullable=False)
    found_items: Mapped[int] = mapped_column(Integer, nullable=True)
    added_items: Mapped[int] = mapped_column(Integer, nullable=True)
    collection_duration: Mapped[int] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(Enum(CollectionStatus), nullable=False, default=CollectionStatus.INIT)
    time_added: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())



    def __repr__(self) -> str:
        return f"CollectionTask: '{self.task_name}' / {self.platform}. ({self.status.name})"


class DBPost(Base):
    __tablename__ = 'post'

    __table_args__ = (
        UniqueConstraint('post_url', name='uq_post_url'),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    platform_id: Mapped[str] = mapped_column(String(50), nullable=True, unique=False)
    post_url: Mapped[str] = mapped_column(String(60), nullable=False)
    date_created: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    post_type: Mapped[PyEnum] = mapped_column(Enum(PostType), nullable=False)
    content: Mapped[dict] = Column(JSON)
    date_collected: Mapped[datetime] = mapped_column(DateTime, nullable=False, server_default=func.now())

    # todo: temp nullable
    collection_task_id: Mapped[int] = mapped_column(ForeignKey("collection_task.id"), nullable=True)
    # collection_step: Mapped[int] = mapped_column(Integer, nullable=True)

    # user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    # user: Mapped[DBUser] = relationship(back_populates="posts")

    # content_schema_id: Mapped[int] = mapped_column(ForeignKey("post_content_schema.id"), nullable=True)
    # content_schema: Mapped[DBPostContentSchema] = relationship(back_populates="posts")

    comments: Mapped[list[DBComment]] = relationship(back_populates="post")

