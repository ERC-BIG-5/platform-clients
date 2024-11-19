from enum import Enum, auto
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_PATH = Path("/home/rsoleyma/projects/platforms-clients")
BASE_DATA_PATH = PROJECT_PATH / "data"
MISC_PATH = BASE_DATA_PATH / "misc"
CLIENTS_TASKS_PATH = BASE_DATA_PATH / "tasks"
PROCESSED_TASKS_PATH = BASE_DATA_PATH / "processed_tasks"
CLIENTS_DATA_PATH = BASE_DATA_PATH / "clients"

LOG_CONFIG_FILE = BASE_DATA_PATH / "logging.json"
ENV_FILE_PATH = (PROJECT_PATH / ".env").absolute().as_posix()

MAIN_DIRS = [BASE_DATA_PATH, CLIENTS_TASKS_PATH, PROCESSED_TASKS_PATH, MISC_PATH]

for dir in MAIN_DIRS:
    dir.mkdir(exist_ok=True)

class Big5Config(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE_PATH, env_file_encoding='utf-8', extra='allow')

    db_type: Literal["sqlite"] = Field(alias="DB_TYPE", default="sqlite")
    moved_processed_tasks: bool = Field(alias="MOVE_PROCESSED_TASKS", default=True)
    reset_db: bool = Field(alias="RESET_DB", default=False)


BIG5_CONFIG = Big5Config()


class CollectionStatus(Enum):
    INIT = auto()
    ACTIVE = auto()  # started, but not currently running
    RUNNING = auto()  # started and currently running
    PAUSED = auto()  # if it's set to pause
    ABORTED = auto()  # started and aborted
    DONE = auto()  # started and finished

class PostType(Enum):
    REGULAR = auto()