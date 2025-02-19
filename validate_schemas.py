import json
import os
from pathlib import Path
import sys

import json5
import jsonschema
from tqdm.contrib.concurrent import process_map


IGNORE_PATHS = [
    "node_modules",
    "site-packages",
    "draft2019-09",
    "draft2020-12",
    "draft-next",
    "vendor",
]


def process_file(schema_file):
    # Calculate the path of the new file
    new_schema_file = Path("valid_data", *schema_file.parts[1:])

    # Skip any directories named with .json at the end
    if not schema_file.is_file() and not new_schema_file.is_file():
        return

    # Skip files in ignored directories
    for path in IGNORE_PATHS:
        if path in schema_file.parts or path in new_schema_file.parts:
            return

    try:
        schema = json5.load(open(schema_file))
    except ValueError:
        return

    # Skip meta schemas
    if schema.get("$id").startswith("https://json-schema.org/"):
        return

    vcls = jsonschema.validators.validator_for(schema)
    try:
        vcls.check_schema(schema)
    except jsonschema.exceptions.SchemaError:
        return

    new_schema_file = Path("valid_data", *schema_file.parts[1:])
    Path.mkdir(new_schema_file.parent, parents=True, exist_ok=True)
    json.dump(schema, open(new_schema_file, "w"), sort_keys=True, indent=2)


if __name__ == "__main__":
    # Increase the recursion limit to handle large schemas
    sys.setrecursionlimit(10000)

    data_path = Path("fetched_data")
    process_map(process_file, list(data_path.rglob("*.json")), chunksize=10)
