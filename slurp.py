import argparse
import csv
import os
import json
import sys
import time

import requests
import requests_ratelimiter
import tqdm


def slurp(outfile):
    query = 'count:all file:\.json$ (content:\'{\n  "$schema": "https://json-schema.org/\' or content:\'{\n    "$schema": "https://json-schema.org/\')'

    print(query)

    session = requests.Session()
    adapter = requests_ratelimiter.LimiterAdapter(per_second=2)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    matches = 0

    with session.get(
        "https://sourcegraph.com/.api/search/stream",
        params={"q": query},
        headers={
            "Accept": "text/event-stream",
            "Authorization": "token " + os.environ["SRC_ACCESS_TOKEN"],
        },
        stream=True,
    ) as resp, open(outfile, "w") as f:
        pbar = tqdm.tqdm()
        writer = csv.writer(f)
        writer.writerow(
            ["repository", "repoStars", "repoLastFetched", "commit", "path"]
        )
        event = None
        for line in resp.iter_lines():
            if not line:
                continue
            time.sleep(0.1)
            line = line.decode("utf-8").strip()

            if line.startswith("event:"):
                event = line.split(":", maxsplit=1)[1].strip()
                if event != "matches":
                    sys.stderr.write(event + "\n")
            elif line.startswith("data:"):
                data = line.split(":", maxsplit=1)[1].strip()

                if event == "filters":
                    # We don't need to record filtering information
                    continue
                if event == "matches":
                    record = [
                        (
                            m["repository"],
                            m.get("repoStars", ""),
                            m.get("repoLastFetched", ""),
                            m["commit"],
                            m["path"],
                        )
                        for m in json.loads(data)
                    ]
                    writer.writerows(record)
                    matches += len(record)
                    pbar.update(len(record))
                elif event == "progress":
                    sys.stderr.write(data + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--outfile", required=True)
    args = parser.parse_args()

    slurp(args.outfile)