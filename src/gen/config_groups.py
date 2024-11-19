import itertools
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Union

from pydantic import BaseModel


# Example usage
example_config = {
    "time_config": {
        "start": "2024-01-01T00:00:00",
        "end": "2024-01-02T00:00:00",
        "interval": {
            "hours": 1
            # Could also use any combination of timedelta parameters:
            # days, seconds, microseconds, milliseconds, minutes, hours, weeks
        }
    },
    "static_params": {
        "api_key": "your-api-key",
        "format": "json"
    },
    "variable_params": {
        "region": ["us-east", "us-west", "eu-central"],
        "metric": ["cpu", "memory"]
    }
}



# Generate concrete configs
concrete_configs = generate_configs(example_config)

# Print first few configs
print("First few generated configurations:")
for config in concrete_configs[:3]:
    print(json.dumps(config, indent=2))
