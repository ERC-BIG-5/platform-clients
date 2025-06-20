import enum
import json
from pydantic import BaseModel
from big5_databases.databases.external import BASE_DATA_PATH, PostgresConnection, SQliteConnection, DBConfig, ClientTaskConfig, \
    ClientConfig, CollectConfig, CollectionStatus
from src.clients.clients_models import ClientTaskGroupConfig, TimeConfig

"""from datapipeline.loader import ModuleLoader
from datapipeline.models import PipelineConfig, FunctionPointer
from datapipeline.pipeline_methods.abstract_pipeline_method import AbstractPipelineMethod
from datapipeline.pipeline_methods.builtin.data_filter_method import DataFilter
from datapipeline.pipeline_methods.builtin.labelstudio_method import ValueAssignment"""

if __name__ == "__main__":

    main_schema = {"$defs": {}}
    # First add all the base models
    for clz in [ClientTaskConfig, ClientTaskGroupConfig, DBConfig, SQliteConnection, PostgresConnection, ClientConfig,
                CollectConfig, CollectionStatus, TimeConfig]:

        if issubclass(clz, BaseModel):
            # Pydantic Model -> Generate schema using `.model_json_schema()`
            main_schema["$defs"][clz.__name__] = clz.model_json_schema()

        elif issubclass(clz, enum.Enum):
            # Enum Class -> Generate enum schema
            main_schema["$defs"][clz.__name__] = {
                "type": "string",
                "enum": [e.value for e in clz]
            }

    main_schema["$schema"] = "http://json-schema.org/draft-07/schema#"
    main_schema["type"] = "object"
    main_schema["properties"] = {}

    # Add DatabaseConnectionType

    main_schema["$defs"]["DatabaseConnectionType"] = {
        "oneOf": [
            {"$ref": "#/$defs/SQliteConnection"},
            {"$ref": "#/$defs/PostgresConnection"}
        ]
    }

    main_schema["OneOf"] = [
        {"$ref": "#/$defs/ClientTaskConfig"},
        {"$ref": "#/$defs/ClientTaskGroupConfig"},
        {
            "type": "array",
            "items": {"$ref": "#/$defs/ClientTaskConfig"}
        }
    ]

    output_path = BASE_DATA_PATH / "schema/tasks.schema.json"
    json.dump(main_schema, output_path.open("w", encoding="utf-8"),
              indent=2, ensure_ascii=False)
