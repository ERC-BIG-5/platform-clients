from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from tools.env_root import root
from tools.files import read_data

# todo use root() again
PROJECT_PATH = root()
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
    run_config_file_name: str = Field(alias="RUN_CONFIG")
    moved_processed_tasks: bool = Field(alias="MOVE_PROCESSED_TASKS", default=True)
    send_posts: bool = Field(alias="SEND_TASK_POSTS", default=True)
    send_post_host: str = Field(alias="SEND_POST_HOST", default="http://localhost")
    send_post_port: int = Field(alias="SEND_POST_PORT", default=8800)
    send_post_path: str = Field(alias="SEND_POST_PATH", default="")
    # not sure anymore
    db_type: Literal["sqlite"] = Field(alias="DB_TYPE", default="sqlite")
    reset_db: bool = Field(alias="RESET_DB", default=False)
    test_mode : bool = Field(alias="TEST_MODE", default=False)


BIG5_CONFIG = Big5Config()
RUN_CONFIG = read_data(BASE_DATA_PATH / "_RUN_CONFIG" / BIG5_CONFIG.run_config_file_name)


def read_run_config() -> dict:
    return read_data(BASE_DATA_PATH / "_RUN_CONFIG" / BIG5_CONFIG.run_config_file_name)
