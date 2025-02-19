#!/bin/bash

pv more_repo_commits.json |
    jq -r '("https://raw.githubusercontent.com/" + .repository) as $url | .path as $path | .commits[] | $url + "/" + .sha + "/" + $path' |
    while read url; do
        # Strip the url prefix to get the path
        path=$(echo "$url" | cut -d/ -f4-)
        if ! [ -f "more_fetched_data/$path" ]; then
            curl "$url" --silent --create-dirs -o "more_fetched_data/$path"
            sleep 1
        fi
    done