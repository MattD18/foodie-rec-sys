from metaflow import FlowSpec, step, IncludeFile, Parameter

import os
import yaml

from google.cloud import bigquery
from google.oauth2 import service_account

import pandas as pd




class TrainFlow(FlowSpec):
    """
    A flow to train a prediction model using specificed config
    """

    config_path = Parameter(
        "config_path", help="Relative path to config file for training job", default="like_given_impression.yaml"
    )
    
    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.
        """
        # load training config
        with open(self.config_path,'rb') as f:
            self.config = yaml.load(f, Loader=yaml.BaseLoader)

        print("HelloFlow is starting.")
        self.next(self.load_data)

    @step
    def load_data(self):
        """
        A step for loading training data

        Notes:
        -abstract out bigquery connection into seperate class
        -how to handle when training data is too big to fit into memory
        """
        project_id = os.environ['GCP_PROJECT']
        key_path = os.environ['BQ_CREDENTIALS']

        # establish bigquery connection
        credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        # load training data per config
        training_table = self.config['data']['table']
        training_start_ds = self.config['data']['start_ds']
        training_end_ds = self.config['data']['end_ds']
        query = f"""
            select 
                * 
            from warehouse_training_tables.{training_table}
            where ds >= '{training_start_ds}'
                and ds <= '{training_end_ds}'
        """

        self.train_df = client.query(
            query=query
        ).to_dataframe()

        self.next(self.train_model)

    @step
    def train_model(self):
        """
        A step for training model
        """
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.
        """
        print("TrainFlow is all done.")


if __name__ == "__main__":
    TrainFlow()