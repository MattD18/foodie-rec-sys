from metaflow import FlowSpec, step, Parameter

import os
import yaml
import datetime
import json

from google.cloud import bigquery
from google.oauth2 import service_account

import pandas as pd
import numpy as np


class SMSImpressionQualityFlow(FlowSpec):
    """
    Predicts quality of recommendation to user delivered via SMS channel

    Rule-Based Logic:
        Randomly select from restaurants with Google Maps rating b/w 4.5 
        and 4.9 that have not been shown in last 2 prediction lists
    """

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.
        """

        print("SMSImpressionQualityFlow is starting.")
        self.next(self.load_data)

    @step
    def load_data(self):
        """
        A step for loading user impression data and restaurant list
        """
        project_id = os.environ['GCP_PROJECT']
        # establish bigquery connection
        bq_client = bigquery.Client(project=project_id)
        print(os.environ['GOOGLE_APPLICATION_CREDENTIALS'])
        user_query = """
            select
              a.user_id,
              engagement_sms_impression_restaurant_last_2_prediction_lists
            from application.user a
            left join warehouse_feature_generation.application_engagement_features b
            on a.user_id = b.user_id

        """
        self.user_df = bq_client.query(
            query=user_query
        ).to_dataframe()

        restaurant_query = """
            select
                a.id,
                c.restaurant_google_rating_float
            from application.restaurant a
            left join restaurant_data.google_maps_application_id_mapping b
            on a.id = b.application_id
            left join warehouse_feature_generation.google_maps_basic_features c
            on b.google_maps_id = c.restaurant_id

        """
        self.restaurant_df = bq_client.query(
            query=restaurant_query
        ).to_dataframe()

        prediction_query = """
            select
                max(prediction_id) as predcition_id
            from inference.predictions
        """
        self.prediction_id = bq_client.query(
            query=prediction_query
        ).to_dataframe().iloc[0][0] + 1

        self.next(self.inference)

    @step
    def inference(self):
        """
        A step for running inference

        """
        # get eligible restaurant list
        eligibility_mask = (self.restaurant_df['restaurant_google_rating_float'] >= 4.5) \
            & (self.restaurant_df['restaurant_google_rating_float'] <= 4.9)
        restaurant_list = self.restaurant_df[eligibility_mask]['id']
        print(restaurant_list.shape)
        # generate predictions for each user
        prediction_list = []
        for _, record in self.user_df.iterrows():
            user_id = record['user_id']
            engaged_list = record['engagement_sms_impression_restaurant_last_2_prediction_lists']
            predictions = restaurant_list[~restaurant_list.isin(engaged_list)].sample(5).tolist()
            prediction_list.append({
                'user_id':user_id,
                'restaurant_id_list':predictions
            })
        self.prediction_df = pd.DataFrame(prediction_list)
        self.prediction_df['prediction_ts'] = pd.Timestamp.now()
        self.prediction_df['model_id'] = 1
        self.prediction_df['prediction_id'] = self.prediction_id
        self.prediction_df['restaurant_id_list'] = \
            self.prediction_df['restaurant_id_list'].apply(lambda x: json.dumps(x))
        self.next(self.save)

    @step
    def save(self):
        """
        A step to save predictions
        """
        project_id = os.environ['GCP_PROJECT']
        # establish bigquery connection
        bq_client = bigquery.Client(project=project_id)

        
        # upload prediction
        dataset_id = 'inference'
        table_id = 'predictions'

        # tell the client everything it needs to know to upload our csv
        dataset_ref = bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        # Configure the job to overwrite the table if it already exists
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = 'WRITE_APPEND'

        # Upload the DataFrame to BigQuery
        out_df = self.prediction_df
        job = bq_client.load_table_from_dataframe(out_df, table_ref, job_config=job_config)

        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.
        """
        print("SMSImpressionQualityFlow is all done.")


if __name__ == "__main__":
    SMSImpressionQualityFlow()
