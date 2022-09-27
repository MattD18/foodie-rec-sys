from metaflow import FlowSpec, step, Parameter

import os
import yaml

from google.cloud import bigquery
from google.cloud import storage

import pandas as pd
import numpy as np

class DailyRuleBasedZipcodeV0Flow(FlowSpec):
    """
    A flow to produce daily rec list based on users zip code

    Core Logic:
        -for each user:
            -Filter on restaurants with no user impression in last X days
            -randomly choose Y restaurants from user's zipcode
            -randomly choose Z restauranst from outside user's zipcode
    """

    config_path = Parameter(
        "config_path",
        help="Relative path to config file for inference job",
        default="daily_rule_based_zipcode_v0.yaml"
    )

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.
        """
        # load training config
        with open(self.config_path, 'rb') as f:
            self.config = yaml.load(f, Loader=yaml.BaseLoader)

        print("DailyRuleBasedZipcodeV0Flow is starting.")
        self.next(self.load_data)

    @step
    def load_data(self):
        """
        A step for loading user impression data and restaurant list

        Notes:
        -abstract out bigquery connection into seperate class
        -how to handle when training data is too big to fit into memory
        -down the line don't need to save data, pass reference?
        """
        project_id = os.environ['GCP_PROJECT']
        # establish bigquery connection
        bq_client = bigquery.Client(project=project_id)

        user_query = """
            select
                user_id,
                user_home_zipcode_id,
                list_impression_r7d_restaurant_ids
            from warehouse_feature_store.user_sparse_features
            where ds = (select max(ds) from warehouse_feature_store.user_sparse_features)
        """

        self.user_df = bq_client.query(
            query=user_query
        ).to_dataframe()

        restaurant_query = """
            select
                restaurant_id,
                restaurant_zipcode_id,
            from warehouse_feature_store.object_sparse_features
            where ds = (select max(ds) from warehouse_feature_store.object_sparse_features)
        """
        self.restaurant_df = bq_client.query(
            query=restaurant_query
        ).to_dataframe()

        self.next(self.train_model)

    @step
    def inference(self):
        """
        A step for running inference

        """
        prediction_list = []
        for _, record in self.user_df.iterrows():
            # get user info
            user_id = record['user_id']
            user_zip_id = record['user_home_zipcode_id']
            user_impression_restaurant_list = \
                record['list_impression_r7d_restaurant_ids']
            # filter on unseen restaurants
            impression_mask = self.restaurant_df['restaurant_id'] \
                .isin(user_impression_restaurant_list)
            eligible_restaurant_df = self.restaurant_df.loc[~impression_mask]
            in_zip_mask = \
                eligible_restaurant_df['restaurant_zipcode_id'] == user_zip_id
            # sample restaurants in zipcode
            in_zip_eligible_restaurant_df = eligible_restaurant_df[in_zip_mask]
            in_zip_n = min(2, in_zip_eligible_restaurant_df.shape[0])
            in_zip_selection = eligible_restaurant_df['restaurant_id'].sample(in_zip_n)
            # sample restaurants out of zipcode
            out_zip_eligible_restaurant_df = eligible_restaurant_df[~in_zip_mask]
            out_zip_n = min(2, out_zip_eligible_restaurant_df.shape[0])
            out_zip_selection = eligible_restaurant_df['restaurant_id'].sample(out_zip_n)
            # TODO fill in remainder if necessary
            # shuffle selection list
            user_selection = in_zip_selection + out_zip_selection
            np.random.shuffle(user_selection)
            # append to output
            prediction_list.append({
                'user_id': user_id,
                'restaurant_list': user_selection
            })
        # format into dataframe
        self.prediction_df = pd.DataFrame(prediction_list)  
        self.next(self.save)

    @step
    def save(self):
        """
        A step to save predictions
        """
        # upload self.prediction_df
        # need to figure out location
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.
        """
        print("DailyRuleBasedZipcodeV0Flow is all done.")


if __name__ == "__main__":
    DailyRuleBasedZipcodeV0Flow()
