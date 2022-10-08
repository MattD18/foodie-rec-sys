from metaflow import FlowSpec, step, Parameter

import os
import yaml
import datetime

from google.cloud import bigquery

import pandas as pd
import numpy as np


class DailyRuleBasedNeighborhoodV0Flow(FlowSpec):
    """
    A flow to produce daily rec list based on users neighborhood

    Core Logic:
        -for each user:
            -Filter on restaurants with no user impression in last X days
            -randomly choose Y restaurants from user's neighborhood
            -randomly choose Z restauranst from outside user's neighborhood
    """

    config_path = Parameter(
        "config_path",
        help="Relative path to config file for inference job",
        default="daily_rule_based_neighborhood_v0.yaml"
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

        print("DailyRuleBasedNeighborhoodV0Flow is starting.")
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
                user_neighborhood_id
            from warehouse_feature_store.user_sparse_neighborhood_id
            where ds = (select max(ds) from warehouse_feature_store.user_sparse_neighborhood_id)
        """

        self.user_df = bq_client.query(
            query=user_query
        ).to_dataframe()

        restaurant_query = """
            select
                restaurant_id,
                restaurant_neighborhood_id,
            from warehouse_feature_store.object_sparse_restaurant_neighborhood_id
            where ds = (select max(ds) from warehouse_feature_store.object_sparse_restaurant_neighborhood_id)
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
            user_neighborhood_id = record['user_neighborhood_id']
            # TODO: filter on unseen restaurants in last X days
            eligible_restaurant_df = self.restaurant_df.copy(deep=True)
            user_neighborhood_mask = \
                eligible_restaurant_df['restaurant_neighborhood_id'] == user_neighborhood_id
            # sample restaurants in neighborhood
            in_neighborhood_restaurant_df = \
                eligible_restaurant_df[user_neighborhood_mask]
            n_in_neighborhood = min(
                n_in_zip_target, 
                in_neighborhood_restaurant_df.shape[0]
            )
            in_neighborhood_selection = \
                in_neighborhood_restaurant_df['restaurant_id'] \
                .sample(n_in_neighborhood).to_list()
            # sample restaurants out of neighborhood
            out_neighborhood_restaurant_df = \
                eligible_restaurant_df[~user_neighborhood_mask]
            n_out_neighborhood = min(
                max(n - len(in_neighborhood_selection), n - n_in_zip_target),
                out_neighborhood_restaurant_df.shape[0]
            )
            out_neighborhood_selection = \
                out_neighborhood_restaurant_df['restaurant_id'] \
                .sample(n_out_neighborhood).to_list()
            # shuffle selection list
            user_selection = \
                in_neighborhood_selection + out_neighborhood_selection
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
        if pred_check.iloc[0][0] != ds:
            # prepare prediction_df for upload
            self.prediction_df['ds'] = ds
            self.prediction_df['model_type'] = 'rule_based_neighborhood'
            self.prediction_df['model_id'] = 0
            # self.prediction_df['restaurant_list'] = \
            #     self.prediction_df['restaurant_list'] \
            #         .apply(lambda x: ', '.join([str(y) for y in x]))
            ordered_cols = [
                'ds',
                'user_id',
                'restaurant_list',
                'model_type',
                'model_id',
            ]
            self.prediction_df = self.prediction_df[ordered_cols]
            # upload table, TODO abstract table_schema to shared location
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
                    "type": "INTEGER",
                    "mode": "REPEATED"
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
        else:
            print(f"{ds} already exists")
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.
        """
        print("DailyRuleBasedNeighborhoodV0Flow is all done.")


if __name__ == "__main__":
    DailyRuleBasedNeighborhoodV0Flow()
