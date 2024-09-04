import asyncio
import inspect
from abc import ABC, abstractmethod
from typing import TypeVar

from pydantic import BaseModel
from pydantic_settings import BaseSettings

from src.clients.clients_models import CollectConfig
from src.db.db_models import DBPost, DBUser

ClientConfig = TypeVar("ClientConfig", bound=BaseModel)
PostEntry = TypeVar("PostEntry")
UserEntry = TypeVar("UserEntry")


class AbstractClient[ClientConfig, PostEntry](ABC):

    def __init__(self, config: dict | BaseModel | BaseSettings):
        self.config = config
        if inspect.iscoroutinefunction(self.setup):
            asyncio.run(self.setup())
        else:
            self.setup()

    @abstractmethod
    def setup(self):
        pass

    @abstractmethod
    def transform_config(self, abstract_config: CollectConfig) -> ClientConfig:
        pass

    @abstractmethod
    async def collect(self, collect_settings: CollectConfig) -> list[PostEntry]:
        pass

    @abstractmethod
    def create_post_entry(self, post: PostEntry) -> DBPost:
        pass

    @abstractmethod
    def create_user_entry(self, user: UserEntry) -> DBUser:
        pass

    @property
    @abstractmethod
    def platform_name(self)-> str:
        pass

