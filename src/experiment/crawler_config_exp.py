from copy import deepcopy
from sys import platform
from deepdiff import DeepDiff, DeepHash, Delta
from deepdiff.serialization import json_dumps, json_loads

from src.clients.clients_models import ClientTaskConfig

base_config = ClientTaskConfig(
    platform="twitter",
    task_name="test_crawler",
    base_collection_config= {
        "until":"2023-01-01",
        "region": "spain"
    },
    collection_steps=[]
)

def base_hash(config: ClientTaskConfig):
    config_data = config.model_dump()
    return DeepHash(config_data)[config_data]

if __name__ == "__main__":
    print(base_hash(base_config))

    base_col_orig_config_data = base_config.base_collection_config
    base_col_mod_data = deepcopy(base_col_orig_config_data)
    base_col_mod_data["until"] = "2023-01-02"
    diff = DeepDiff(base_col_orig_config_data,base_col_mod_data)
    delta = Delta(diff, serializer=json_dumps)
    print(delta.dumps())