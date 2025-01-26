import json

from src.clients.clients_models import RunConfig
from src.const import BASE_DATA_PATH, PROJECT_PATH

if __name__ == "__main__":

    run_config_schema_file = BASE_DATA_PATH / "schema/runconfig.schema.json"
    run_config_schema_file.write_text(json.dumps(RunConfig.model_json_schema(), indent=2, ensure_ascii=False))
    print("RunConfig schema created and stored in", run_config_schema_file.relative_to(PROJECT_PATH))
