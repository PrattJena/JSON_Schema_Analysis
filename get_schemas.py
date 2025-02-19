import os
import requests


def encode_url(url):
    # Encode special characters in the URL
    encoded_url = (url.replace("https://", "https__slash__slash_")
                   .replace("http://", "http__slash__slash_")
                   .replace("/", "__slash__")
                   .replace(":", "__colon__")
                   .replace("?", "__question__"))

    # Ensure only one .json at the end if it already exists
    if encoded_url.endswith(".json"):
        return encoded_url
    else:
        return encoded_url + ".json"


def read_urls_from_file(filepath):
    # Read URLs from the provided file
    with open(filepath, 'r', encoding='utf-8') as file:
        urls = [line.strip() for line in file if line.strip()]
    return urls


def download_json_schema(url_list, folder_name="schemas", failed_urls_file="failed_urls.txt"):
    # Create the folder if it doesn't exist
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    failed_urls = []

    for url in url_list:
        try:
            # Encode the URL for the filename
            filename = encode_url(url)
            filepath = os.path.join(folder_name, filename)

            # Download the content from the URL
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors

            # Save the content to a file
            with open(filepath, 'w', encoding='utf-8') as file:
                file.write(response.text)

            print(f"Downloaded and saved: {filename}")

        except requests.exceptions.RequestException as e:
            print(f"Failed to download {url}: {e}")
            failed_urls.append(url)

    # Save failed URLs to a separate file
    if failed_urls:
        with open(failed_urls_file, 'w', encoding='utf-8') as file:
            for failed_url in failed_urls:
                file.write(f"{failed_url}\n")
        print(f"Failed URLs have been saved to {failed_urls_file}")


# Read the URLs from the file and download them
file_path = 'json-schema-ids.txt'  # Replace with your file path if different
url_list = read_urls_from_file(file_path)
download_json_schema(url_list)
