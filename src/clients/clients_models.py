from pathlib import Path

import math
from datetime import datetime
from typing import Optional, Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator,ValidationInfo, SecretStr, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.const import CollectionStatus, ENV_FILE_PATH

class EmptyModel(BaseModel):
    pass

class CollectConfig(BaseModel):
    model_config = {'extra': "allow"}
    query: Optional[str] = None
    limit: Optional[int] = math.inf
    from_time: Optional[str] = None
    to_time: Optional[str] = None
    language: Optional[str] = None
    location_base: Optional[str] = None
    location_mod: Optional[str] = None


class SQliteConnection(BaseModel):
    db_path: str

    @computed_field
    @property
    def connection_str(self) -> str:
        return f"sqlite:///{self.db_path}"

class PostgresConnection(BaseModel):
    db_name: str
    db_user: str
    db_password: SecretStr
    db_host: str
    db_port: int = 5432

class DBConfig(BaseModel):
    model_config = {'extra': "forbid", "from_attributes": True}
    db_connection: SQliteConnection | PostgresConnection
    name: Optional[str] = None
    is_default: bool = Field(False)
    reset_db: bool = False

    @computed_field
    @property
    def connection_str(self) -> str:
        return self.db_connection.connection_str

    @computed_field
    @property
    def db_type(self)-> Literal["sqlite","postegres"]:
        return "sqlite" if isinstance(self.db_connection, SQliteConnection) else "postgres"


class ClientConfig(BaseModel):
    model_config = {'extra': "forbid", "from_attributes": True}
    auth_config: Optional[dict[str, str]] = None
    request_delay: Optional[int] = 0
    db_config: Optional[DBConfig] = None

class RunConfig(BaseModel):
    model_config = {'extra': "forbid", "from_attributes": True}
    clients: dict[str, ClientConfig]

class ClientTaskConfig(BaseModel):
    model_config = {'extra': "forbid", "from_attributes": True}
    task_name: str
    id: Optional[int] = Field(None, init=False)
    platform: str
    database: Optional[str] = None # default the same as platform
    collection_config: CollectConfig
    client_config: Optional[ClientConfig] = Field(default_factory=ClientConfig)
    transient: bool = False # will be deleted after done
    #
    test: bool = False
    overwrite: bool = False
    #
    status: CollectionStatus = Field(CollectionStatus.INIT, init=False)
    time_added: Optional[datetime] = Field(None, init=False)

    def __repr__(self):
        return f"Collection-Task: {self.task_name} ({self.platform})"


class TimeConfig(BaseModel):
    start: str  # ISO format timestamp
    end: str  # ISO format timestamp
    interval: dict[str, int]  # maps directly to timedelta kwargs


class ClientTaskGroupConfig(BaseModel):
    # one file, many tasks
    platform: str
    group_prefix: str
    id: Optional[int] = Field(None, init=False)
    time_config: TimeConfig
    static_params: dict[str, Any]  # Parameters that stay constant
    variable_params: dict[str, list[Any]] = Field(default_factory=dict)  # Parameters to permute
    store_as_group: bool = False
    database: Optional[str] = Field("")  # default the same as platform

    test: bool = False
    overwrite: bool = False

    # todo. why is this not called?!
    @field_validator("database", mode="before")
    @classmethod
    def set_database(cls, v, info: ValidationInfo) -> str:
        if v is None:
            return info.data["platform"]
        return v

    @model_validator(mode="after")
    def validate_model(cls, model: "ClientTaskGroupConfig"):
        if not model.database:
            model.database = model.platform
        return model

class BaseEnvSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH,
                                      case_sensitive=True,
                                      env_file_encoding='utf-8',
                                      extra='allow')

class RunConfigModel(BaseModel):
    clients: dict[str, ClientConfig] = Field(default_factory=dict)
