from metaflow import FlowSpec, step, Parameter

import os
import yaml
import datetime
import json

from google.cloud import bigquery
from google.oauth2 import service_account

import pandas as pd
import numpy as np


class SMSDailyRecFlow(FlowSpec):
    """
    Predicts daily rec via SMS channel

    Rule-Based Logic:
        Randomly select from restaurants that user has not seen before
    """

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.
        """

        print("SMSDailyRecFlow is starting.")
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
              a.id as user_id,
              engagement_sms_impression_restaurant_list
            from application.dim_user a
            left join warehouse_feature_generation.application_engagement_features_v2 b
            on a.id = b.user_id

        """
        self.user_df = bq_client.query(
            query=user_query
        ).to_dataframe()

        restaurant_query = """
            select
                id,
                name
            from application.dim_restaurant

        """
        self.restaurant_df = bq_client.query(
            query=restaurant_query
        ).to_dataframe()

        self.next(self.inference)

    @step
    def inference(self):
        """
        A step for running inference

        """
        # generate predictions for each user
        prediction_list = []
        for _, record in self.user_df.iterrows():
            user_id = record['user_id']
            engaged_list = record['engagement_sms_impression_restaurant_list']
            eligibility_mask = ~self.restaurant_df['id'].isin(engaged_list)
            prediction = self.restaurant_df[eligibility_mask].sample(1).iloc[0]
            prediction_list.append({
                'ts' : pd.Timestamp.now(),
                'user' : user_id,
                'restaurant_id' :prediction['id'],
                'name' :prediction['name']
            })
        self.prediction_df = pd.DataFrame(prediction_list)
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
        table_id = 'sms_daily_rec_predictions'

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
        print("SMSDailyRecFlow is all done.")


if __name__ == "__main__":
    SMSDailyRecFlow()
