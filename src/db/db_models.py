from datetime import datetime
from enum import Enum as PyEnum

from sqlalchemy import Column, String, DateTime, ForeignKey, JSON, Integer, Enum
# from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, Mapped, mapped_column

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


class CollectionStatus(PyEnum):
    INIT = "init"
    STARTED = "started"
    RUNNING = "running"
    ABORTED = "aborted"
    DONE = "done"


class CollectionTask(Base):
    __tablename__ = 'collection_task'
    id: Mapped[int] = mapped_column(primary_key=True)
    task_name: Mapped[str] = mapped_column(String(50), nullable=False, unique=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    total_steps: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(Enum(CollectionStatus), nullable=False)


class DBPost(Base):
    __tablename__ = 'post'

    id: Mapped[int] = mapped_column(primary_key=True)
    platform: Mapped[str] = mapped_column(String(20), nullable=False)
    # platform_id: Mapped[str] = mapped_column(String(50), nullable=True)
    post_url: Mapped[str] = mapped_column(String(60), nullable=True)
    date_created: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    post_type: Mapped[str] = mapped_column(String(20), nullable=False)
    content: Mapped[dict] = Column(JSON)
    date_collected: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # todo: temp nullable
    collection_task_id: Mapped[int] = mapped_column(ForeignKey("collection_task.id"), nullable=True)
    collection_step: Mapped[int] = mapped_column(Integer, nullable=True)

    # user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    # user: Mapped[DBUser] = relationship(back_populates="posts")

    # content_schema_id: Mapped[int] = mapped_column(ForeignKey("post_content_schema.id"), nullable=True)
    # content_schema: Mapped[DBPostContentSchema] = relationship(back_populates="posts")

    comments: Mapped[list[DBComment]] = relationship(back_populates="post")
