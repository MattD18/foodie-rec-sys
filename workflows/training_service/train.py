from metaflow import FlowSpec, step, IncludeFile, Parameter

import os
import yaml
import time
import pickle

from google.cloud import bigquery
from google.cloud import storage

import pandas as pd

import ranking
from ranking.utils import get_model_from_config_spec
from ranking.constants import MODEL_STORE

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

        print("TrainFlow is starting.")
        self.next(self.load_data)

    @step
    def load_data(self):
        """
        A step for loading training data

        Notes:
        -abstract out bigquery connection into seperate class
        -how to handle when training data is too big to fit into memory
        -down the line don't need to save data, pass reference?
        """
        project_id = os.environ['GCP_PROJECT']
        # establish bigquery connection
        bq_client = bigquery.Client(project=project_id)

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

        self.train_df = bq_client.query(
            query=query
        ).to_dataframe()

        self.next(self.train_model)

    @step
    def train_model(self):
        """
        A step for training model

        Note: 
        need to figure out how to install local ranking modellibrary with dependencies
        os pip install ? w/ dependencies in setup file
        """
        model_arch = self.config['model']['model_arch']
        model = get_model_from_config_spec(model_arch)
        model.fit(self.train_df)
        # save trained model to model store        
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(MODEL_STORE)
        ts = int(time.time())
        self.model_save_path = f"{self.config['model']['model_arch'].lower()}_{ts}.pickle"
        blob = bucket.blob(self.model_save_path)
        serialized_model = pickle.dumps(model)
        blob.upload_from_string(serialized_model)
        
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