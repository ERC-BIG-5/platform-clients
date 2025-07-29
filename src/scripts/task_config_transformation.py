import json
from typing import Any, Optional

from big5_databases.databases.external import CollectConfig
from src.clients.task_parser import parse_task_data
from src.const import ALL_PLATFORMS
from src.platform_orchestration import get_client_class


def transform(abstract_config: CollectConfig, platforms: Optional[list[str]] = None) -> dict[str, Any]:
    if not platforms:
        platforms = ALL_PLATFORMS

    platform_configs = {}

    for platform in platforms:
        mgmt_clz = get_client_class(platform)
        platform_configs[platform] = mgmt_clz.transform_config_to_serializable(abstract_config).model_dump(
            exclude_defaults=True)
    return platform_configs


if __name__ == "__main__":
    conf = {
        "platform": "youtube",
        "group_prefix": "phase_2_1000d",
        "static_params": {
            "limit": 50,
            "language": "en"
        },
        "time_config": {
            "start": "2023-01-01",
            "end": "2023-01-02",
            "interval": {
                "days": 1
            }
        }
    }
    confs_ = parse_task_data(conf)
    for conf_ in confs_:
        platform_configs = transform(conf_.collection_config)
        for platform, config in platform_configs.items():
            if platform is not "youtube":
                continue
            try:
                print(platform)
                print(json.dumps(config, indent=2))
            except TypeError as err:
                print(f"{platform}: {err}")

    tasks = parse_task_data([
        {
            "platform": "tiktok",
            "group_prefix": "phase_2_1000d_0523",
            "static_params": {
                "limit": 1000
            },
            "time_config": {
                "start": "2023-05-01",
                "end": "2023-05-02",
                "interval": {
                    "days": 1
                }
            }
        },
        {
            "platform": "tiktok",
            "group_prefix": "phase_2_1000d_0623",
            "static_params": {
                "limit": 1000
            },
            "time_config": {
                "start": "2023-06-01",
                "end": "2023-06-03",
                "interval": {
                    "days": 1
                }
            }
        }
    ])
    for task in tasks:
        print(task.collection_config.from_time, task.collection_config.to_time)
