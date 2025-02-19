import json
import os
from pathlib import Path
import re
import sys
from urllib.request import urlretrieve

import fasttext
import tqdm


LANG_THRESHOLD = 0.1
FASTTEXT_MODEL_URL = (
    "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin"
)
JSON_SCHEMA_KEYWORDS = {
    "$anchor",
    "$comment",
    "$defs",
    "$dynamicAnchor",
    "$dynamicRef",
    "$id",
    "$recursiveAnchor",
    "$recursiveRef",
    "$ref",
    "$schema",
    "$vocabulary",
    "additionalItems",
    "additionalProperties",
    "allOf",
    "anyOf",
    "const",
    "contains",
    "contentEncoding",
    "contentMediaType",
    "contentSchema",
    "definitions",
    "dependencies",
    "dependentRequired",
    "dependentSchemas",
    "description",
    "disallow",
    "divisibleBy",
    "else",
    "enum",
    "exclusiveMaximum",
    "exclusiveMinimum",
    "extends",
    "format",
    "id",
    "if",
    "items",
    "maxContains",
    "maximum",
    "maxItems",
    "maxLength",
    "maxProperties",
    "minContains",
    "minimum",
    "minItems",
    "minLength",
    "minProperties",
    "multipleOf",
    "not",
    "oneOf",
    "pattern",
    "patternProperties",
    "prefixItems",
    "properties",
    "propertyNames",
    "required",
    "then",
    "title",
    "type",
    "unevaluatedItems",
    "unevaluatedProperties",
    "uniqueItems",
}

IGNORE_KEYWORDS = {
    "$id",
    "$schema",
    "$vocabulary",
    "format",
    "pattern",
    "type",
}


# Adapted from https://stackoverflow.com/a/37697078/123695
def identifier_split(id_str):
    return id_str
    return " ".join(
        re.sub("([A-Z][a-z]+)", r"_\1", re.sub("([A-Z]+)", r"_\1", id_str)).split("_")
    )


def collect_text(schema):
    """Generate a string of text from a schema, ignoring keywords"""
    text = ""

    if isinstance(schema, dict):
        for k, v in schema.items():
            # Ignore some keywords completely
            if k in IGNORE_KEYWORDS:
                continue

            # If the key is not a keyword, include it
            if k not in JSON_SCHEMA_KEYWORDS:
                text += " " + identifier_split(k)
            text += collect_text(v)

    elif isinstance(schema, list):
        text += " ".join(collect_text(v) for v in schema)

    elif isinstance(schema, str):
        # Include any found string values
        text += " " + schema

    return text.replace("\n", " ")


def get_languages(text):
    return {l.split("_")[-1]: p for (l, p) in zip(*model.predict(text, k=5))}


if __name__ == "__main__":
    # Download the language model if needed
    if not os.path.isfile("lid.176.bin"):
        urlretrieve(FASTTEXT_MODEL_URL, "lid.176.bin")
    model = fasttext.load_model("lid.176.bin")

    files = list(Path("valid_data").rglob("*.json"))
    for f in tqdm.tqdm(files):
        if not f.is_file():
            continue

        schema = json.load(f.open(encoding="utf-8"))
        schema_str = collect_text(schema)
        langs = get_languages(schema_str)
        top_lang, prob = max(langs.items(), key=lambda x: x[1])
        if prob < LANG_THRESHOLD:
            top_lang = None
        obj = {
            "repository": "/".join(f.parts[1:3]),
            "commit": f.parts[3],
            "path": str(Path(*f.parts[4:])),
            "language": top_lang,
            "languages": langs,
        }
        json.dump(obj, sys.stdout)
        sys.stdout.write("\n")
