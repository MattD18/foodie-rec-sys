import environ
import io
import os
import json
import random
import requests
import time
import re
from datetime import datetime
from bs4 import BeautifulSoup


from google.cloud import secretmanager
from google.cloud import bigquery
from google.cloud import storage

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

if __name__ == "__main__":

    # get existing guides
    query = """
        SELECT distinct guide_url from restaurant_data.infatuation_guides
    """

    scraped_guides = client.query(
        query=query
    ).to_dataframe()['guide_url'].tolist()
    print(len(scraped_guides))
    
    # get link list for new guides
    download_from_gcs(
        bucket_name=INFATUATION_GCS_BUCKET, 
        source_blob_name=INFATUATION_GCS_BLOB, 
        destination_file_name=INFATUATION_LOCAL_FILE
    )
    with open(INFATUATION_LOCAL_FILE, 'r') as json_file:
        new_guides = json.load(json_file)
        print(len(new_guides))
    
    # take set difference for new links and randomly sample subset for scraping
    candidate_guides = list(set(new_guides).difference(set(scraped_guides)))
    sample_size = min(len(candidate_guides), 2)
    candidate_guides = random.sample(candidate_guides, sample_size)
    print(len(candidate_guides))
    # iterate through links
    # extract guide + restaurant data 

    guide_record_list = []
    restaurant_record_dict = {}
    skipped_urls = []


    guide_title_class_pattern = re.compile(r'.*styles_title.*')
    date_div_pattern = re.compile(r'styles_contributorsList.*')
    guide_body_class_pattern = re.compile(r'.*styles_postContent.*flatplan_body.*')
    restaurant_title_pattern = re.compile(r'.*flatplan_venue-heading.*')
    restaurant_div_pattern = re.compile(r'.*styles_venueContainer.*')
    restaurant_desc_pattern = re.compile(r'.*chakra-text.*')
    perfect_for_class_pattern = re.compile(r'.*flatplan_perfectFor.*')
    perfect_for_span_class_pattern = re.compile(r'.*perfectForTag.*')
    cuisine_tag_pattern = re.compile(r'.*cuisineTag.*')
    neighborhood_tag_pattern = re.compile('.*neighborhoodTag.*')

    for i, url in enumerate(candidate_guides):
        print(url)
        # sleep for random amount of time
        time_to_sleep = random.uniform(1, 5)
        print(f"Sleeping for {time_to_sleep:.2f} seconds")
        #TODO: Uncomment 
        # time.sleep(time_to_sleep)
        
        headers = {
            'User-Agent': get_random_user_agent(user_agents)
        }

        # download guide via request API
        page = requests.get(url, headers=headers)
        soup = BeautifulSoup(page.content, "html.parser")
        # import pdb;pdb.set_trace()
        
        # Extract guide data
        guide_body_div = soup.find('div', class_=guide_body_class_pattern)
        guide_record = {
            'title':soup.find('span', class_=guide_title_class_pattern).getText(),
            'publish_date': soup.find('div', class_=date_div_pattern).find('p').getText(),
            'description_list':[guide_body_div.find('p').getText()],
            'guide_url':url,
        }
        
        # Extract restaurant data        
        restaurant_names = [c.text for c in guide_body_div.find_all('h2', class_=restaurant_title_pattern)]
        restaurant_divs = dict(zip(
            restaurant_names,
            guide_body_div.find_all('div', class_=restaurant_div_pattern)
        ))
        restaurant_descriptions = dict(zip(
            restaurant_names,
            [p.getText() for p in guide_body_div.find_all('p', class_=restaurant_desc_pattern, recursive=False)[1:]]
        ))
        assert len(restaurant_names) == len(restaurant_divs) == len(restaurant_descriptions)
        # TODO: find ratings logic
    #     rating_divs = guide_body_div.find_all('div', 'styles_badge__cDu6b styles_standalone__TQgI0 styles_rating__0wWpz')
    #     restaurant_rating_dict = dict(zip(
    #     [re.match(r'(.*) ([a-z]*) image$', x.parent.find('img', 'styles_image__520x0').attrs['alt']).group(1) for x in rating_divs],
    #     [x.text for x in rating_divs]
    #     ))

        # initialize restaurant data structure
        num_restaurants = len(restaurant_names)
        restaurant_records = {}
        for rn in restaurant_names:
            restaurant_records[rn]={}

        # populate restaurant data structure
        
        for r in restaurant_records:
            r_div = restaurant_divs.get(r)
            tag_set = r_div.find_all('p', {'data-testid': 'tags-groupTag'})
            r_perfect_for_div = r_div.find('div', class_=perfect_for_class_pattern)
            r_cusine_tags = r_div.find_all('span', class_=cuisine_tag_pattern)
            r_neighborhood_tags = r_div.find_all('span', class_=neighborhood_tag_pattern)
            restaurant_records.get(r)['description_list'] = [restaurant_descriptions.get(r)]
            restaurant_records.get(r)['tags'] = []
            restaurant_records.get(r)['review_link'] = None
            restaurant_records.get(r)['cusine'] = [c.getText() for c in r_cusine_tags]
            # import pdb;pdb.set_trace()
            restaurant_records.get(r)['perfect_for'] = [span.getText() for span in r_perfect_for_div.find_all('span', class_=perfect_for_span_class_pattern)]
            restaurant_records.get(r)['price'] = [span.get('data-price') for span in r_div.find_all('span') if 'data-price' in span.attrs]
            
            restaurant_records.get(r)['neighborhood'] = [n.getText() for n in r_neighborhood_tags]
            restaurant_records.get(r)['address'] = []
            # restaurant_records.get(r)['rating'] = restaurant_rating_dict.get(r) #TODO
            restaurant_records.get(r)['rating'] = None
            restaurant_records.get(r)['guide_link'] = url
        
        restaurant_record_dict.update(restaurant_records)
        guide_record_list.append(guide_record)

    import pdb; pdb.set_trace()



  
        # restaurant_counter = -1
        # guide_flag = True
        # for content in guide_body_div.children:
        
    #         if restaurant_counter >= 0:
    #             guide_flag = False
    #         if content.name in ('p', 'div'):
    #             if content['class'][0] == 'styles_text__HThtH':
    #                 if guide_flag:
    #                     guide_record['description_list'].append(content.text)
    #                 else:
    #                     restaurant_records[restaurant_name]['description_list'].append(
    #                         content.text
    #                     )
    #             elif content['class'][0] == 'styles_venueContainer__PbujA':
    #                 restaurant_counter+=1
    #                 restaurant_name = restaurant_names[restaurant_counter]
    #                 for c in content.children:
    #                     if c.name== 'div':
    #                         pass
    #                     elif c.name=='a':
    #                         restaurant_records[restaurant_name]['review_link'] = c.attrs['href']
    #                     elif c.name=='section':
    #                         for tag in c.find_all('a', {'data-testid':'tag-tagLink'}):
    #                             if 'cuisines' in tag.attrs['href']:
    #                                 restaurant_records[restaurant_name]['tags'].append(tag.text)
    #                             elif 'neighborhoods' in tag.attrs['href']:
    #                                 restaurant_records[restaurant_name]['neighborhood'] = tag.text
    #                             elif 'perfect-for' in tag.attrs['href']:
    #                                 restaurant_records[restaurant_name]['perfect_for'].append(tag.text)
    #                                 restaurant_records[restaurant_name]['price'] = re.search(
    #                                     r'.*styles_price([0-9])_.*',
    #                                     ' '.join(c.find('h4', {'class': re.compile(r'.*styles_price([0-9])_.*')}).attrs['class']),
    #                                 ).group(1)
    #                         restaurant_records[restaurant_name]['address'] = c.find('a', 'chakra-heading styles_address__rVOZD css-1v90wif').text
    #         else:
    #             pass
    #     restaurant_record_dict.update(restaurant_records)
    #     guide_record_list.append(guide_record)








    


    # # append to the respective tables 


