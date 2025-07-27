import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import itertools
from pydantic_core._pydantic_core import ValidationError
from big5_databases.databases.external import ClientTaskConfig
from src.clients.clients_models import TimeConfig, ClientTaskGroupConfig, all_task_schemas
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


def generate_configs(config: ClientTaskGroupConfig) -> tuple[ClientTaskGroupConfig, list[ClientTaskConfig]]:
    """Generate all concrete configurations from the config file."""

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

    # use the first platform and create copies for each platform at the end
    multiple_platforms = isinstance(config.platform, list)
    base_platform = config.platform[0] if multiple_platforms else config.platform

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
            if config.test_data:
                conf["test_data"] = config.test_data

            concrete_config = {
                "task_name": f"{config.group_prefix}_{task_no}",
                "collection_config": conf,
                "platform": base_platform,
                # "database": config.database,
                "transient": config.transient,
                "test": config.test,
                "overwrite": config.overwrite,

            }
            concrete_configs.append(ClientTaskConfig.model_validate(concrete_config))
            task_no += 1

    if multiple_platforms:
        # iterate through the other platforms

        for platform in config.platform[1:]:
            # iterate through the first set of configs and add copies with this platform to the list
            for ct_idx in range(task_no):
                new_ct = concrete_configs[ct_idx].model_copy(update={"platform": platform}, deep=True)
                concrete_configs.append(new_ct)

    return config, concrete_configs


def load_tasks_file(task_path: Path) -> list[ClientTaskConfig]:
    """
    Load a validate a task file
    :param task_path: absolute or relative path (to CLIENTS_TASKS_PATH)
    :return: task objects, or group (for permanent-storage) and client configs
    """
    abs_task_path = get_abs_path(task_path, CLIENTS_TASKS_PATH)
    data = read_data(abs_task_path)
    all_tasks = parse_task_data(data)
    for t in all_tasks:
        t.source_file = task_path
    return all_tasks

def parse_task_data(data: dict |  list | all_task_schemas) -> list[ClientTaskConfig]:
    if not isinstance(data, all_task_schemas):
        task_configs = all_task_schemas.model_validate(data)
    else:
        task_configs = data
    all_tasks = []
    if not isinstance(task_configs.root, list):
        task_configs = [task_configs.root]
    else:
        task_configs = task_configs.root

    for conf in task_configs:
        if isinstance(conf, ClientTaskConfig):
            all_tasks.append(conf)

        if isinstance(conf, ClientTaskGroupConfig):
            group_conf, group_tasks = generate_configs(conf)
            all_tasks.extend(group_tasks)

    return all_tasks
