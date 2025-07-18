import json
from typing import Any, Optional

from big5_databases.databases.external import CollectConfig
from src.const import ALL_PLATFORMS
from src.platform_orchestration import get_client_class


def transform(abstract_config: CollectConfig, platforms: Optional[list[str]] = None) -> dict[str, Any]:
    if not platforms:
        platforms = ALL_PLATFORMS

    platform_configs = {}

    for platform in platforms:
        mgmt_clz = get_client_class(platform)
        platform_configs[platform] = mgmt_clz.transform_config_to_serializable(abstract_config).model_dump(exclude_defaults=True)
    return platform_configs


if __name__ == "__main__":
    conf = {
        "platform": "tiktok",
        "is_random": True,
        "limit": 20,
        "from_time": "2023-01-02",
        "to_time": "2023-12-31"
    }
    conf_ = CollectConfig.model_validate(conf)
    platform_configs = transform(conf_)
    for platform,config in platform_configs.items():
        try:
            print(platform)
            print(json.dumps(config, indent=2))
        except TypeError as err:
            print(f"{platform}: {err}")