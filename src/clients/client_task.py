from dataclasses import dataclass
from pathlib import Path
from typing import Sequence, Optional

from src.clients.clients_models import ClientTaskConfig
from src.const import CLIENTS_TASKS_PATH, BIG5_CONFIG, PROCESSED_TASKS_PATH
from src.db import db_funcs
from src.db.db_models import CollectionTask
from src.misc.files import get_abs_path, read_data
from src.misc.project_logging import get_b5_logger

logger = get_b5_logger(__file__)


def load_task(task_path: Path) -> ClientTaskConfig:
    abs_task_path = get_abs_path(task_path, CLIENTS_TASKS_PATH)
    return ClientTaskConfig.model_validate(read_data(abs_task_path))


def check_new_client_tasks():
    for file in CLIENTS_TASKS_PATH.glob("*.json"):
        task = load_task(file)
        processed = db_funcs.add_db_collection_task(task)
        if processed and BIG5_CONFIG.moved_processed_tasks:
            file.rename(PROCESSED_TASKS_PATH / file.name)


def get_platforms_task_queue(platforms: Optional[Sequence[str]] = None) -> dict[str, CollectionTask]:
    """
    todo: dont use the db model class...
    :param platforms:
    :return:
    """
    tasks = db_funcs.get_task_queue(platforms)
    platform_grouped: dict[str, CollectionTask] = {}
    for task in tasks:
        platform_grouped.setdefault(task.platform, []).append(task)
    print(platform_grouped)


@dataclass
class ClientTask:
    file_path: Path
    task_config: ClientTaskConfig

    def __post_init__(self):
        self.task_config = load_task(self.file_path)

    def check_crawling_status(self):
        pass
