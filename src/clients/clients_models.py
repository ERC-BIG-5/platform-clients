from typing import Optional, Any

from typing import Optional, Any

from pydantic import BaseModel, Field, field_validator, model_validator, ValidationInfo
from pydantic_settings import BaseSettings, SettingsConfigDict

from databases.external import ClientConfig
from src.const import ENV_FILE_PATH


class EmptyModel(BaseModel):
    pass


class RunConfig(BaseModel):
    model_config = {'extra': "forbid", "from_attributes": True}
    clients: dict[str, ClientConfig]


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
    test_data: Optional[list[dict]] = None

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
