import json
import argparse
import sys
import os
import time
import requests
import requests_ratelimiter
import csv
import tqdm


def get_id(file_path):
    ids = set()

    with open(file_path, 'r') as file:
        file_content = file.read()
        json_documents = file_content.splitlines()
        for document in json_documents:
            try:
                data = json.loads(document)
                if isinstance(data, dict) and '$id' in data:
                    id_value = data['$id']
                    if not id_value.startswith(("http://json-schema.org", "https://json-schema.org")):
                        ids.add(id_value)
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON document: {document}")

    return ids


def load_links_from_file(file_path):
    with open(file_path, 'r') as file:
        urls = {line.strip() for line in file}
    return urls


def get_repos(outfile, url):
    query = f'count:all file:\\.json$ content:\'"$schema": "{url}\''
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
    ) as resp, open(outfile, "a", newline='') as f:  # Open in append mode
        pbar = tqdm.tqdm()
        writer = csv.writer(f)

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
                    continue
                if event == "matches":
                    record = [
                        (
                            m["repository"],
                            m.get("repoStars", ""),
                            m.get("repoLastFetched", ""),
                            m["commit"],
                            m["path"],
                            url
                        )
                        for m in json.loads(data)
                    ]
                    writer.writerows(record)
                    matches += len(record)
                    pbar.update(len(record))
                elif event == "progress":
                    sys.stderr.write(data + "\n")


def store_repos(schema_file, links_file, outfile):
    # Create or overwrite the file and write the header
    with open(outfile, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(
            ["repository", "repoStars", "repoLastFetched", "commit", "path", "url"]
        )

    # ids = get_id("schemas.json")
    ids = get_id(schema_file)
    # links = load_links_from_file("json-schema-ids.txt")
    links = load_links_from_file(links_file)
    total_urls = ids.union(links)

    for url in total_urls:
        get_repos(outfile, url)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--outfile", required=True)
    args = parser.parse_args()
    store_repos("schemas.json", "json-schema-ids.txt", outfile=args.outfile)

