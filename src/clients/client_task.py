from dataclasses import dataclass
from pathlib import Path

from src.clients.clients_models import ClientTaskConfig
from src.const import CLIENTS_TASKS_PATH
from src.misc.files import get_abs_path, read_data
from src.misc.project_logging import get_b5_logger

logger = get_b5_logger(__file__)

def load_task(task_path: Path) -> ClientTaskConfig:
    abs_task_path = get_abs_path(task_path, CLIENTS_TASKS_PATH)
    return ClientTaskConfig.model_validate(read_data(abs_task_path))

def check_new_tasks():
    for file in CLIENTS_TASKS_PATH.glob("*.json"):
        print(file)
        logger.debug(f"")
        load_task(file)



@dataclass
class CrawlerTask:
    file_path: Path
    task_config: ClientTaskConfig

    def __post_init__(self):
        self.task_config = load_task(self.file_path)

    def check_crawling_status(self):
        pass


