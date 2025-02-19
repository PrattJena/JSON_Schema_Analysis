import csv
import json
import os
import sys

import requests
import requests_ratelimiter
import tqdm


def get_license(session, repo):
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": "Bearer " + os.environ["GITHUB_TOKEN"],
        "X-GitHub-Api-Version": "2022-11-28",
    }

    try:
        r = session.get(
            "https://api.github.com/repos/" + repo + "/license",
            headers=headers,
            timeout=10,
        )
    except (
        requests.exceptions.ConnectionError,
        requests.exceptions.ReadTimeout,
    ):
        # Skip on request error
        return None
    else:
        # Get the commit hashes
        obj = r.json()
        if obj.get("license"):
            return obj["license"]["spdx_id"]
        return None


def main():
    # Initialize a new session
    session = requests.Session()
    adapter = requests_ratelimiter.LimiterAdapter(per_second=2)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Get the already fetched repositories if they exist
    fetched_repos = set()
    if os.path.exists("licenses.json"):
        for line in open("licenses.json", "r"):
            obj = json.loads(line)
            fetched_repos.add(obj["repository"])

    with open("repos.csv", "r") as csvfile:
        # Count number of rows and reset
        reader = csv.DictReader(csvfile)
        repos = (
            set(row["repository"].split("/", maxsplit=1)[1] for row in reader)
            - fetched_repos
        )

        for repo in tqdm.tqdm(repos):
            license = get_license(session, repo)
            obj = {"repository": repo, "license": license}
            json.dump(obj, sys.stdout)
            sys.stdout.write("\n")


if __name__ == "__main__":
    main()
