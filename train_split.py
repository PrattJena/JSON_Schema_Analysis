import argparse
import copy
import gzip
import json
import os
from pathlib import Path
import sys

import numpy as np
import pybktree
from sklearn.model_selection import GroupShuffleSplit
import tqdm
import unionfind
import Levenshtein


PERMISSIVE_LICENSES = set(json.load(open("permissive_licenses.json")))


def files_list(licenses):
    data_path = Path("valid_data")
    files = [
        f
        for f in data_path.rglob("*.json")
        if f.is_file() and licenses["/".join(f.parts[1:3])] in PERMISSIVE_LICENSES
    ]
    return files


def write_schemas(filename, schema_list, schema_data):
    sys.stderr.write(f"Writing {filename}…\n")
    with gzip.open(Path("data") / filename, "wt") as f:
        for schema in tqdm.tqdm(list(schema_list)):
            filename = str(os.path.join(*Path(schema).parts[1:]))

            # Skip schemas that have not been fetched this run
            try:
                data = schema_data[filename]
            except KeyError:
                continue

            schema = open(schema).read()

            # Get stars or null if missing
            try:
                repoStars = int(data["repoStars"])
            except (KeyError, ValueError):
                repoStars = None

            obj = {
                "repository": data["repository"],
                "commit": data["commit"],
                "commitDate": data["commitDate"],
                "path": data["path"],
                "repoStars": repoStars,
                "repoLastFetched": data["repoLastFetched"],
                "content": schema,
                "license": data["license"],
                "language": data["language"],
            }
            json.dump(obj, f)
            f.write("\n")


def get_repo_data(file, key):
    data = {}
    with open(file, "r") as f:
        for line in f:
            obj = json.loads(line)
            data[obj["repository"]] = obj[key]

    return data


def main(similarity, split, seed, commits_file, licenses_file, languages_file):
    licenses = get_repo_data(licenses_file, "license")
    languages = get_repo_data(languages_file, "language")
    files = files_list(licenses)

    # Prepare a BK Tree if we're doing similarity grouping
    if similarity:
        tree = pybktree.BKTree(
            lambda a, b: Levenshtein.distance(a, b) / max(len(a), len(b))
        )

    # Initialize a union-find data structure
    uf = unionfind.UnionFind()

    # Track the first schema added to each org so we can group them
    org_map = {}

    sys.stderr.write("Grouping by repository…\n")
    for schema_file in tqdm.tqdm(files):
        path_str = str(schema_file)

        # Get the organization name from the path
        org = schema_file.parts[1:3]

        uf.add(str(schema_file))
        if org not in org_map:
            # Track the first schema for this organization
            org_map[org] = str(schema_file)
        else:
            # Merge with the previous group if this
            # organization has been seen before
            uf.union(org_map[org], str(schema_file))

        # Add to the BK Tree
        if similarity:
            tree.add((str(schema_file), open(schema_file).read().strip()))

    del org_map

    # Optionally group together similar files
    if similarity:
        sys.stderr.write("Grouping similar files…\n")
        for schema_file in tqdm.tqdm(files):
            path_str = str(schema_file)
            data = open(schema_file).read().strip()

            # Find similar schemas for this schema and group them together
            for other_path, _ in tree.find(data, similarity):
                uf.union(path_str, other_path)

    # Produce a list of schemas and their associated groups
    all_schemas = list()
    schema_groups = list()
    for group, schemas in enumerate(uf.components()):
        all_schemas.extend(schemas)
        schema_groups.extend([group] * len(schemas))

    # Split the schemas into training and test
    all_schemas = np.array(all_schemas)
    schema_groups = np.array(schema_groups)
    gss = GroupShuffleSplit(n_splits=1, train_size=split, random_state=seed)
    (train_indexes, test_indexes) = next(gss.split(all_schemas, groups=schema_groups))

    test_schemas = all_schemas[test_indexes]
    test_groups = schema_groups[test_indexes]
    gss = GroupShuffleSplit(n_splits=1, train_size=0.5, random_state=seed)
    (test_indexes, val_indexes) = next(gss.split(test_schemas, groups=test_groups))

    schema_data = {}
    with open(commits_file) as f:
        for line in f:
            obj = json.loads(line)
            for commit in obj["commits"]:
                obj = copy.deepcopy(obj)
                filename = os.path.join(obj["repository"], commit["sha"], obj["path"])
                obj["commit"] = commit["sha"]
                obj["commitDate"] = commit["date"]
                obj["license"] = licenses[obj["repository"]]
                obj["language"] = languages.get(obj["repository"])
                schema_data[filename] = obj

    # Write the train and test sets
    write_schemas("train.jsonl.gz", all_schemas[train_indexes], schema_data)
    write_schemas("test.jsonl.gz", test_schemas[test_indexes], schema_data)
    write_schemas("validation.jsonl.gz", test_schemas[val_indexes], schema_data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--similarity", default=None, type=float)
    parser.add_argument("--seed", default=38, type=int)
    parser.add_argument("--split", default=0.8, type=float)
    parser.add_argument("--commits_file", default="commits.json")
    parser.add_argument("--licenses_file", default="licenses.json")
    parser.add_argument("--languages_file", default="languages.json")
    args = parser.parse_args()
    main(
        args.similarity,
        args.split,
        args.seed,
        args.commits_file,
        args.licenses_file,
        args.languages_file,
    )
