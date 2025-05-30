"""
a json file in data: platform_quotas.json
which has <platform_name>:<quota_halt_ts> pairs
"""
import json
from datetime import datetime
from pathlib import Path

from src.const import BASE_DATA_PATH


def fp() -> Path:
    return BASE_DATA_PATH / "platform_quotas.json"

def store(current : dict[str, datetime]) -> None:
    dump_format = {p: t.timestamp() for p, t in current.items()}
    fp().write_text(json.dumps(dump_format))

def load_quotas() -> dict[str, datetime]:
    if fp().exists():
        json_data = json.load(fp().open())
        json_data = {k: datetime.fromtimestamp(t) for k, t in json_data.items()}
        return json_data
    else:
        return {}


def store_quota(platform: str, time: datetime) -> None:
    current = load_quotas()
    current[platform] = time
    store(current)


def remove_quota(platform: str) -> None:
    current = load_quotas()
    if platform in current:
        del current[platform]
    store(current)
