from typing import Optional

import itertools
from datetime import datetime, timedelta

from src.clients.clients_models import TimeConfig, ClientTaskGroupConfig, ClientTaskConfig


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
            conf['from_time'] = timestamp.isoformat()
            conf['to_time'] = (timestamp + interval).isoformat()

            concrete_config = {
                "task_name": f"{config.group_prefix}_{task_no}",
                "collection_config": conf,
                "platform": config.platform,
                "database": config.database,
                "transient": config.store_as_group,
                "test": config.test,
                "overwrite":config.overwrite
            }
            concrete_configs.append(ClientTaskConfig.model_validate(concrete_config))
            task_no += 1

    if config.store_as_group:
        return config, concrete_configs

    return None, concrete_configs
