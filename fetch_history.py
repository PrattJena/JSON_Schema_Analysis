import csv
import json
import os
import sys

import requests
import requests_ratelimiter
import tqdm


def get_commits(session, repo, path):
    query = {"path": path}
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": "Bearer " + os.environ["GITHUB_TOKEN"],
        "X-GitHub-Api-Version": "2022-11-28",
    }

    try:
        r = session.get(
            "https://api.github.com/repos/" + repo + "/commits",
            params=query,
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
        if isinstance(obj, list):
            commits = []
            for c in obj:
                try:
                    commits.append(
                        {"sha": c["sha"], "date": c["commit"]["committer"]["date"]}
                    )
                except KeyError:
                    pass
            return commits
        else:
            return None


def main():
    # Initialize a new session
    session = requests.Session()
    adapter = requests_ratelimiter.LimiterAdapter(per_second=2)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    with open("more_repos_with_json_schema.csv", "r") as csvfile:
        # Count number of rows and reset
        reader = csv.DictReader(csvfile)
        rows = sum(1 for row in reader)
        csvfile.seek(0)

        reader = csv.DictReader(csvfile)
        for row in tqdm.tqdm(reader, total=rows):
            # Remove github.com/ from the beginning and fetch commits
            repo = row["repository"].split("/", maxsplit=1)[1]
            commits = get_commits(session, repo, row["path"])

            # Write the collected commits
            if commits:
                obj = {
                    "repository": repo,
                    "path": row["path"],
                    "repoStars": row["repoStars"],
                    "repoLastFetched": row["repoLastFetched"],
                    "commits": list(commits),
                }
                json.dump(obj, sys.stdout)
                sys.stdout.write("\n")


if __name__ == "__main__":
    main()
