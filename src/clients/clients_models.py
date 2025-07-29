from typing import Optional, Any

from pydantic import BaseModel, Field, RootModel
from pydantic_settings import BaseSettings, SettingsConfigDict

from big5_databases.databases.external import ClientConfig, ClientTaskConfig
from src.const import ENV_FILE_PATH


class RunConfig(BaseModel):
    model_config = {'extra': "forbid", "from_attributes": True}
    clients: dict[str, ClientConfig]


class TimeConfig(BaseModel):
    start: str  # ISO format timestamp
    end: str  # ISO format timestamp
    interval: dict[str, int]  # maps directly to timedelta kwargs
    timespan: Optional[dict[str, int]] = None  # instead of duration with the same as the interval use end_i - timespan
    clamp_to_same_day: bool = False
    truncate_overflow: bool = Field(False,
                                    description="does not include the 'last' timespan, if the 'ts' + 'interval' > end. Works very well with 'clamp_to_same_day' to cover a whole month 01-01 - 02-01 gets all days of february")


class ClientTaskGroupConfig(BaseModel):
    # one file, many tasks
    platform: str | list[str]
    group_prefix: str
    time_config: TimeConfig
    static_params: dict[str, Any]  # Parameters that stay constant
    variable_params: dict[str, list[Any]] = Field(default_factory=dict)  # Parameters to permute
    # store_as_group: bool = False
    id: Optional[int] = Field(None, init=False)
    transient: Optional[bool] = False
    force_new_index: Optional[bool] = Field(False,
                                            description="When starting indices from 0, does not work, we look for next free indices")

    test: bool = False
    overwrite: bool = False
    test_data: Optional[list[dict]] = None


class BaseEnvSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH,
                                      case_sensitive=True,
                                      env_file_encoding='utf-8',
                                      extra='allow')


class RunConfigModel(BaseModel):
    clients: dict[str, ClientConfig] = Field(default_factory=dict)


all_task_schemas = RootModel[ClientTaskConfig | ClientTaskGroupConfig | list[ClientTaskConfig | ClientTaskGroupConfig]]
