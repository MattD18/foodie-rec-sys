from google.cloud import storage
import json
import requests
import random
import time
from bs4 import BeautifulSoup

# List of user-agent strings
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/89.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15'
]

# Function to get a random user-agent
def get_random_user_agent(user_agents):
    return random.choice(user_agents)

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def download_from_gcs(bucket_name, source_blob_name, destination_file_name):
    """Downloads a file from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"Blob {source_blob_name} downloaded to {destination_file_name}.")

INFATUATION_GCS_BUCKET = 'foodie-infatuation-data'
INFATUATION_GCS_BLOB = 'infatuation_link_list.json'
INFATUATION_LOCAL_FILE = 'data/infatuation_link_list.json'
INFATUATION_URL = 'https://www.theinfatuation.com/new-york/guides'

if __name__ == "__main__":

    # sleep for random amount of time
    time_to_sleep = random.uniform(1, 10800)
    print(f"Sleeping for {time_to_sleep:.2f} seconds")
    time.sleep(time_to_sleep)
    # Home page URL scraping
    print('Scrape NY homepage')
    headers = {
        'User-Agent': get_random_user_agent(user_agents)
    }
    page = requests.get(INFATUATION_URL, headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")
    # Find all links with the specified pattern
    print('Extract guide links')
    links = soup.find_all('a', href=lambda href: href and href.startswith("/new-york/guides/"))
    links = ['https://www.theinfatuation.com' + x['href'] for x in links]
    print(len(links))
    # fetch existing URL list
    print('download existing list')
    download_from_gcs(
        bucket_name=INFATUATION_GCS_BUCKET, 
        source_blob_name=INFATUATION_GCS_BLOB, 
        destination_file_name=INFATUATION_LOCAL_FILE
    )
    with open(INFATUATION_LOCAL_FILE, 'r') as json_file:
        existing_links = json.load(json_file)
        print(len(existing_links))
    # merge links
    print('add new links to list')
    updated_links = list(set(links + existing_links))
    print(len(updated_links))
    # upload updated links list
    print('upload links')
    with open(INFATUATION_LOCAL_FILE, 'w') as json_file:
        json.dump(updated_links, json_file)
    upload_to_gcs(
        bucket_name=INFATUATION_GCS_BUCKET, 
        source_file_name=INFATUATION_LOCAL_FILE, 
        destination_blob_name=INFATUATION_GCS_BLOB
    )


