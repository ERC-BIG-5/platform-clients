from typing import Optional, Literal

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


class ClientTaskConfig(BaseModel):
    platform: str
    task_name: str
    base_collection_config: CollectConfig
    collection_steps: Optional[list[dict]] = Field(default_factory=list)
    auth_config: Optional[dict] = None
