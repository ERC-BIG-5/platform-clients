import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import itertools
from pydantic_core._pydantic_core import ValidationError

from big5_databases.databases.external import ClientTaskConfig
from src.clients.clients_models import TimeConfig, ClientTaskGroupConfig
from src.const import CLIENTS_TASKS_PATH
from tools.files import read_data, get_abs_path

logger = logging.getLogger(__name__)


def generate_timestamps(time_config: TimeConfig) -> list[datetime]:
    """Generate list of timestamps from start to end with given interval."""
    # there is also pd.date_range which can be tried out
    start = datetime.fromisoformat(time_config.start)
    end = datetime.fromisoformat(time_config.end)
    interval = timedelta(**time_config.interval)

    timestamps = []
    current = start
    while current <= end:
        timestamps.append(current)
        current += interval

    return timestamps


def generate_configs(config: ClientTaskGroupConfig) -> tuple[Optional[ClientTaskGroupConfig], list[ClientTaskConfig]]:
    """Generate all concrete configurations from the config file."""
    # Load and parse config file

    # Generate all timestamps
    timestamps = generate_timestamps(config.time_config)

    # Generate all permutations of variable parameters
    param_names = config.variable_params.keys()
    param_values = config.variable_params.values()
    param_permutations = list(itertools.product(*param_values))
    logger.info(
        f"group will create {len(param_permutations) * len(timestamps)} tasks (var. permutations: {len(param_permutations)})")

    # Generate all concrete configs
    concrete_configs: list[ClientTaskConfig] = []
    task_no = 0
    for timestamp in timestamps:
        for param_combination in param_permutations:
            # Start with static parameters
            conf = config.static_params.copy()

            conf.update(dict(zip(param_names, param_combination)))

            # Add time parameters
            interval = timedelta(**config.time_config.interval)

            if config.time_config.timespan:
                timespan_ = timedelta(**config.time_config.timespan)
                if timespan_ == interval:
                    logging.getLogger("src.platform_orchestration").info(
                        f"interval and timespan are equal. Using interval would be sufficient")
                conf['from_time'] = (timestamp + interval - timespan_).isoformat()
            else:
                conf['from_time'] = timestamp.isoformat()
            conf['to_time'] = (timestamp + interval).isoformat()
            conf["test_data"] = config.test_data

            concrete_config = {
                "task_name": f"{config.group_prefix}_{task_no}",
                "collection_config": conf,
                "platform": config.platform,
                "database": config.database,
                "transient": config.store_as_group,
                "test": config.test,
                "overwrite": config.overwrite,

            }
            concrete_configs.append(ClientTaskConfig.model_validate(concrete_config))
            task_no += 1

    if config.store_as_group:
        return config, concrete_configs

    return None, concrete_configs


def load_tasks(task_path: Path) -> tuple[list[ClientTaskConfig], Optional[ClientTaskGroupConfig]]:
    """
    Load a validate a task file
    :param task_path: absolute or relative path (to CLIENTS_TASKS_PATH)
    :return: task objects, or group (for permanent-storage) and client configs
    """
    abs_task_path = get_abs_path(task_path, CLIENTS_TASKS_PATH)
    data = read_data(abs_task_path)
    ct_cfg_err = None

    if isinstance(data, list):
        parsed_tasks = []
        for task_data in data:
            task = ClientTaskConfig.model_validate(task_data)
            task.source_file = task_path
            parsed_tasks.append(task)
        return parsed_tasks, None
    try:
        task = ClientTaskConfig.model_validate(data)
        task.source_file = task_path
        return [task], None
    except ValidationError as v_err:
        ct_cfg_err = v_err

    try:
        ctg = ClientTaskGroupConfig.model_validate(data)
        task_group_conf, tasks = generate_configs(ctg)
        for t in tasks:
            t.source_file = task_path
        return tasks, task_group_conf
    except ValidationError as v_err:
        print(ct_cfg_err)
        print("****")
        print(v_err)
        logger.error("Task file cannot be parsed neither as TaskConfig nor as TaskGroupConfig")
        return [], None
