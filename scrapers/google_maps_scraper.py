import environ
import io
import os
import requests
import json
from datetime import datetime

import pandas as pd

from google.cloud import secretmanager
from google.cloud import bigquery

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

env = environ.Env(DEBUG=(bool, False))
env_file = os.path.join(BASE_DIR, ".env")

if os.path.isfile(env_file):
    # Use a local secret file, if provided
    env.read_env(env_file)
elif os.environ.get("GOOGLE_CLOUD_PROJECT", None):
    # Pull secrets from Secret Manager
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")

    client = secretmanager.SecretManagerServiceClient()
    settings_name = os.environ.get("SETTINGS_NAME", "django_settings")
    name = f"projects/{project_id}/secrets/{settings_name}/versions/latest"
    payload = client.access_secret_version(name=name).payload.data.decode("UTF-8")

    env.read_env(io.StringIO(payload))
else:
    raise Exception("No local .env or GOOGLE_CLOUD_PROJECT detected. No secrets found.")

API_KEY = os.environ['GOOGLE_MAPS_API_KEY']
PROJECT_ID = os.environ['PROJECT_ID']

# create a client instance for your project
client = bigquery.Client(project=PROJECT_ID, location="US")


def get_query_content(query):
    r = requests.get(query)
    content = json.loads(r.content)
    return content


def get_nearby_places(lat, long, radius=100):
    keyword = 'restaurant'
    location = f'{lat},{long}'
    query = f'https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={location}&radius={radius}&keyword={keyword}&key={API_KEY}'
    content = get_query_content(query)
    return content, query


def get_place_details(place_id):
    field_list = [
        'formatted_address', 'name', 'place_id', 'business_status',
        'url', 'website', 'editorial_summary', 'price_level', 'rating',
        'user_ratings_total', 'geometry'
    ]

    fields = ','.join(field_list)
    query = f'https://maps.googleapis.com/maps/api/place/details/json?fields={fields}&place_id={place_id}&key={API_KEY}'
    content = get_query_content(query)
    return content, query


def get_field(field, content):
    if field in content:
        if field == 'editorial_summary':
            out = content[field]['overview']
        else:
            out = content[field]
    else:
        out = None
    return out


def get_geostring(geometry_data):
    geo_string = None
    if geometry_data is not None:
        lat = geometry_data['location']['lat']
        lng = geometry_data['location']['lng']
        geo_string = f'POINT({lng} {lat})'
    return geo_string

def upload_csv_to_bq(filename, dataset_id, table_id):
    '''
        uploads CSV to bigquery, precondition, table already exists
    '''
    # tell the client everything it needs to know to upload our csv
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True
    job_config.write_disposition = 'WRITE_TRUNCATE'
    # load the csv into bigquery
    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # Waits for table load to complete.
    # looks like everything worked :)
    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))


query_records = []
place_records = []

if __name__ == "__main__":

    # get list of query points
    query_points = [
        (40.71159018842078, -73.94526673018571)
    ]

    # iterate through query points
    for lat, long in query_points:

        # get list of restaurants from each query
        place_list, place_query = get_nearby_places(lat, long)
        # store query metadata
        next_page_token = get_field('next_page_token', place_list)
        status = get_field('status', place_list)
        print(status)
        place_list = place_list['results']
        print(len(place_list))
        place_query = place_query[:-44]  # removes API key
        # write api call to query record
        query_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        query_records.append({
            'query_url': place_query,
            'next_page_token': next_page_token,
            'status': status,
            'ts': query_ts,
        })

        # iterate through resturants
        for place in place_list:
            # pull restaurant details
            place_id = place['place_id']
            details, detail_query = get_place_details(place_id)
            details = details['result']
            # write place details to places record
            detail_updated_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            place_records.append({
                'place_id': get_field('place_id', details),
                'name': get_field('name', details),
                'formatted_address': get_field('formatted_address', details),
                'website': get_field('website', details),
                'rating': get_field('rating', details),
                'user_ratings_total': get_field('user_ratings_total', details),
                'price_level': get_field('price_level', details),
                'business_status': get_field('business_status', details),
                'editorial_summary': get_field('editorial_summary', details),
                'url': get_field('url', details),
                'geo': get_geostring(get_field('geometry', details)),
                'place_query': place_query,
                'updated_at': detail_updated_ts
            })

    # save query results to csv
    query_df = pd.DataFrame(query_records)
    query_df.to_csv('data/google_places_queries.csv', index=False)
    # save places detail to csv
    place_df = pd.DataFrame(place_records)
    place_df.to_csv('data/google_places.csv', index=False)      

    # TODO upload dataframe to raw logs table
    upload_csv_to_bq(
        filename='data/google_places_queries.csv',
        dataset_id='restaurant_data',
        table_id='google_maps_query_logs'
    )
    upload_csv_to_bq(
        filename='data/google_places.csv',
        dataset_id='restaurant_data', table_id='google_maps_place_logs'
    )
