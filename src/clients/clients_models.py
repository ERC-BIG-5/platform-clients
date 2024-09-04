from typing import Optional

from pydantic import BaseModel, Field


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
    base_collection_config: CollectConfig
    collection_steps: Optional[list[CollectionStepConfig]] = Field(default_factory=list)
    auth_config: Optional[dict] = None
