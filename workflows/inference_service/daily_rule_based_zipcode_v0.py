from metaflow import FlowSpec, step, Parameter

import os
import yaml
import datetime

from google.cloud import bigquery

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
                home_zipcode_id,
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

        self.next(self.inference)

    @step
    def inference(self):
        """
        A step for running inference

        """
        n = int(self.config['params']['n'])
        n_in_zip_target = int(self.config['params']['n_in_zip'])

        prediction_list = []
        for _, record in self.user_df.iterrows():
            # get user info
            user_id = record['user_id']
            user_zip_id = record['home_zipcode_id']
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
            n_in_zip_ = min(
                n_in_zip_target, 
                in_zip_eligible_restaurant_df.shape[0]
            )
            in_zip_selection = eligible_restaurant_df['restaurant_id'] \
                .sample(n_in_zip_).to_list()
            # sample restaurants out of zipcode
            out_zip_eligible_restaurant_df = \
                eligible_restaurant_df[~in_zip_mask]
            out_zip_n = min(
                max(n - len(in_zip_selection), n - n_in_zip_target),
                out_zip_eligible_restaurant_df.shape[0]
            )
            out_zip_selection = eligible_restaurant_df['restaurant_id'] \
                .sample(out_zip_n).to_list()
            # fill in remainder if necessary
            user_selection = in_zip_selection + out_zip_selection
            if len(user_selection) < n:
                fill_mask = ~self.restaurant_df['restaurant_id'] \
                    .isin(user_selection)
                fill_restaurant_df = self.restaurant_df[fill_mask]
                n_fill = n - len(user_selection)
                fill_selection = fill_restaurant_df['restaurant_id'] \
                    .sample(n_fill).to_list()
                user_selection += fill_selection
            # shuffle selection list
            np.random.shuffle(user_selection)
            # append to output
            prediction_list.append({
                'user_id': user_id,
                'restaurant_list': user_selection
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
        ds = datetime.date.today()
        destination_table = self.config['data']['prediction_table']
        # check that prediction doesn't exist for day
        pred_check_query = f"""
            select
                max(ds)
            from {destination_table}
        """
        pred_check = bq_client.query(query=pred_check_query).to_dataframe()
        pred_check.iloc[0][0] == ds
        # prepare prediction_df for upload
        self.prediction_df['ds'] = ds
        self.prediction_df['model_type'] = 'rule_based_zip_code'
        self.prediction_df['model_id'] = 0
        self.prediction_df['restaurant_list'] = \
            self.prediction_df['restaurant_list'] \
                .apply(lambda x: ', '.join([str(y) for y in x]))
        ordered_cols = [
            'ds',
            'user_id',
            'restaurant_list',
            'model_type',
            'model_id',
        ]
        self.prediction_df = self.prediction_df[ordered_cols]
        # upload table
        table_schema = [
            {
                "name": "ds",
                "type": "DATE",
                "mode": "NULLABLE"
            },
            {
                "name": "user_id",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },
            {
                "name": "restaurant_list",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "model_type",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "model_id",
                "type": "INTEGER",
                "mode": "NULLABLE"
            },
        ]
        self.prediction_df.to_gbq(
            destination_table,
            project_id=project_id,
            if_exists='append',
            table_schema=table_schema
        )
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
