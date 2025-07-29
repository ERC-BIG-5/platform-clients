"""
task related function of the orchestration
"""
from collections import defaultdict

from pathlib import Path
from tools.project_logging import get_model_logger, get_logger
from typing import TYPE_CHECKING, Optional

from big5_databases.databases.external import ClientTaskConfig
from src.clients.task_parser import load_tasks_file
from src.const import CLIENTS_TASKS_PATH, BIG5_CONFIG, PROCESSED_TASKS_PATH

if TYPE_CHECKING:
    from platform_orchestration import PlatformOrchestrator

logger = get_logger(__file__)

class TaskManager:

    def __init__(self, orchestration: "PlatformOrchestrator"):
        self.orchestration = orchestration
        self.logger = get_model_logger(self)

    def check_new_client_tasks(self, task_dir: Optional[Path] = None) -> list[str]:
        """
        check for JSON file in the specific folder and add them into the sdb
        :return: returns a list of task names
        """
        files = []
        if not task_dir:
            task_dir = CLIENTS_TASKS_PATH
        else:
            files.append(task_dir)
        if task_dir.is_dir():
            files = task_dir.glob("*.json")

        added_tasks = []
        for file in files:
            # create collection_task models
            added_tasks.extend(self.handle_task_file(file))
        return added_tasks

    def handle_task_file(self, file: Path) -> list[str]:
        tasks = load_tasks_file(file)
        added_tasks, all_added = self.add_tasks(tasks)

        if all_added and BIG5_CONFIG.moved_processed_tasks:
            file.rename(PROCESSED_TASKS_PATH / file.name)

        logger.info(f"new tasks: # {len(added_tasks)}")
        logger.debug(f"new tasks: # {[t for t in added_tasks]}")
        return added_tasks

    def add_tasks(self,
                  tasks: list[ClientTaskConfig]) -> tuple[list[str], bool]:
        """

        @return: list of task names and if all tasks were added
        """
        added_tasks: list[str] = []
        missing_platform_managers: set[str] = set()

        all_added = True
        grouped_by_platform = defaultdict(list)

        for task in tasks:
            # print(task, missing_platform_managers)
            if task.platform in missing_platform_managers:
                all_added = False
                continue
            if task.platform not in self.orchestration.platform_managers:
                logger.warning(f"No manager found for platform: {task.platform}")
                all_added = False
                missing_platform_managers.add(task.platform)
                continue

            grouped_by_platform[task.platform].append(task)

        for group, g_tasks in grouped_by_platform.items():
            manager = self.orchestration.platform_managers[group]
            if not manager.active:
                self.logger.warning(f"Tasks added to platform {group} is currently not set 'active'")
            added_tasks_names = manager.add_tasks(g_tasks)
            added_tasks.extend(added_tasks_names)
            if len(g_tasks) != len(added_tasks_names):
                logger.warning(
                    f"Not all tasks added for platform: {group}, {len(added_tasks_names)}/{len(g_tasks)}")
                # Register the platform database in main DB
                # self.add_platform_db(task.platform, manager.platform_db.db_config.connection_str)
                all_added = False

        return added_tasks, all_added

    def fix_tasks(self):
        """
        set the tasks of status "RUNNING" to "PAUSED"
        """
        for platform, platform_mgmt in self.orchestration.platform_managers.items():
            platform_mgmt.reset_running_tasks()