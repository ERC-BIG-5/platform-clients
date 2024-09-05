from datetime import datetime
from enum import Enum as PyEnum, auto
from typing import Optional

from pydantic import BaseModel


class CollectionStatus(PyEnum):
    INIT = auto()
    ACTIVE = auto()  # started, but not currently running
    RUNNING = auto()  # started and currently running
    PAUSED = auto()  # if it's set to pause
    ABORTED = auto()  # started and aborted
    DONE = auto()  # started and finished


class CollectConfig(BaseModel):
    query: str
    limit: Optional[int] = 100
    from_time: Optional[str] = None
    to_time: Optional[str] = None
    language: Optional[str] = None
    location_base: Optional[str] = None
    location_mod: Optional[str] = None
    extra: Optional[dict] = None


class CollectionStepConfig(CollectConfig):
    query: Optional[str] = None


class ClientTaskConfig(BaseModel):
    task_name: str
    platform: str
    collection_config: list[CollectionStepConfig]
    auth_config: Optional[dict[str, str]] = None
    model_config = {'from_attributes': True}
    status: CollectionStatus = CollectionStatus.INIT
    time_added: Optional[datetime] = None
