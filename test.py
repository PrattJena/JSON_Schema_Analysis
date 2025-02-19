import gzip
import json
import re
import argparse
import validators
import sys
import os
import requests
import requests_ratelimiter
import csv
import time
import tqdm


def get_content_id(data, json_data_id_list):
    for doc in data:
        if doc:
            try:
                json_data_content = json.loads(json.loads(doc)["content"])
                json_data_content_id = json_data_content.get("$id", None)
                # print(json_data_content_id)
                if validators.url(json_data_content_id):
                    json_data_id_list.append(json_data_content_id)
            except json.JSONDecodeError:
                print("Error decoding JSON document.")

    json_data_id_list[:] = list(set(json_data_id_list))


def get_repos(outfile, content_url):
    # content_query = f'"$schema": "{content_url}'
    # print(content_query)
    query = f'count:all file:\\.json$ content:\'"$schema": "{content_url}\''
    print(query)
    session = requests.Session()
    adapter = requests_ratelimiter.LimiterAdapter(per_second=2)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    matches = 0
    output_file = 'response_output.txt'
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
                            content_url
                        )
                        for m in json.loads(data)
                    ]
                    writer.writerows(record)
                    matches += len(record)
                    pbar.update(len(record))
                elif event == "progress":
                    sys.stderr.write(data + "\n")


def store_repos(outfile):
    data_files = os.listdir("data")
    gz_files = [file for file in data_files if file.endswith('.gz')]
    print(gz_files)
    json_data_id_list = []
    for data_file in gz_files:
        with gzip.open(f"data/{data_file}", 'rt') as gzipped_file:
            unzipped_file = gzipped_file.read()
        json_objects = unzipped_file.split('\n')
        get_content_id(json_objects, json_data_id_list)
    print(len(json_data_id_list))

    # with open(outfile, "a") as file:
    #     for line in json_data_id_list:
    #         file.write(line + "\n")
    get_repos(outfile, "https://json-schema.org/")

    # with requests.get(
    #     "https://sourcegraph.com/.api/search/stream",
    #     params={"q": query},
    #     headers={
    #         "Accept": "text/event-stream",
    #         "Authorization": "token " + os.environ["SRC_ACCESS_TOKEN"],
    #     },
    #     stream=True,
    # ) as resp, open(output_file, 'w') as file:
    #     for line in resp.iter_lines():
    #         if line:
    #             line = line.decode("utf-8").strip()
    #             file.write(line + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--outfile", required=True)
    args = parser.parse_args()
    # print(re.escape("https://identity.foundation/dwn/json-schemas/protocols-configure.json"))
    store_repos(args.outfile)
# print(content)
# for content_id in content:
#     print(content_id)

# output_file_path = 'output.json'

# # List to hold all JSON objects
# json_list = []


# # Split content into individual JSON objects (adjust delimiter if necessary)


# print(json.loads(json_objects[0]))
# # print(json.loads(json_objects[1]))

# for obj in json_objects:
#     if obj.strip():  # Only try to load non-empty objects
#         try:
#             json_data = json.loads(obj)
#             json_list.append(json_data)
#         except json.JSONDecodeError as e:
#             print(f"Error decoding JSON: {e}")

# # Write the list of JSON objects to a new file
# with open(output_file_path, 'w') as output_file:
#     json.dump(json_list, output_file, indent=4)

# print(f"JSON objects written to {output_file_path}")
