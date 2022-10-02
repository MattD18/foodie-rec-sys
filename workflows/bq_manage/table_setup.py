from metaflow import FlowSpec, step

import os

from google.cloud import bigquery


class TableSetup(FlowSpec):
    """
    Flow to create tables not managed through DBT
    """

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.
        """
        self.next(
            self.create_prediction_dataset,
            self.create_demo_landing_dataset,
        )

    @step
    def create_prediction_dataset(self):
        """
        A step for creating prediction tables

        Note:
        have to store restaurant predictions as string, need to revist
        add logic to allow piecewise rebuilding of tables if needed
        """
        project_id = os.environ['GCP_PROJECT']
        # establish bigquery connection
        bq_client = bigquery.Client(project=project_id)

        # create dataset
        dataset_id = "{}.predictions".format(bq_client.project)
        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        # Send the dataset to the API for creation, with an explicit timeout.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
        dataset = bq_client.create_dataset(dataset, timeout=30)
        print("Created dataset {}.{}".format(
            bq_client.project,
            dataset.dataset_id
        ))

        # create table
        dataset_ref = bigquery.DatasetReference(project_id, 'predictions')
        table_ref = dataset_ref.table("user_daily_recs")
        schema = [
            bigquery.SchemaField("ds", "DATE"),
            bigquery.SchemaField("user_id", "INTEGER"),
            bigquery.SchemaField(
                "restaurant_list", "INTEGER", mode="REPEATED"
            ),
            bigquery.SchemaField("model_type", "STRING"),
            bigquery.SchemaField("model_id", "INTEGER"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ds",
        )

        table = bq_client.create_table(table)
        self.next(self.join)

    @step
    def create_demo_landing_dataset(self):
        """
        A step for creating copies of demo tables

        """
        project_id = os.environ['GCP_PROJECT']
        # establish bigquery connection
        bq_client = bigquery.Client(project=project_id)

        # create dataset
        dataset_id = "{}.demo_data".format(bq_client.project)
        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        # Send the dataset to the API for creation, with an explicit timeout.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
        dataset = bq_client.create_dataset(dataset, timeout=30)
        print("Created dataset {}.{}".format(
            bq_client.project,
            dataset.dataset_id
        ))

        # create restaurant table
        dataset_ref = bigquery.DatasetReference(project_id, 'demo_data')
        table_ref = dataset_ref.table("dim_restaurant")
        schema = [
            bigquery.SchemaField("ds", "DATE"),
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("address", "STRING"),
            bigquery.SchemaField("zipcode", "STRING"),
            bigquery.SchemaField("neighborhood", "STRING"),
            bigquery.SchemaField("cuisine", "STRING"),
            bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
            bigquery.SchemaField("price_est", "STRING"),
            bigquery.SchemaField("website_url", "STRING"),
            bigquery.SchemaField("menu_url", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ds",
        )
        table = bq_client.create_table(table)

        # create user table
        dataset_ref = bigquery.DatasetReference(project_id, 'demo_data')
        table_ref = dataset_ref.table("dim_user")
        schema = [
            bigquery.SchemaField("ds", "DATE"),
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("neighborhood", "STRING"),
            bigquery.SchemaField("saved_list", "INTEGER"),
            bigquery.SchemaField("last_login", "TIMESTAMP"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ds",
        )
        table = bq_client.create_table(table)

        # create engagement table
        dataset_ref = bigquery.DatasetReference(
            project_id,
            'demo_data'
        )
        table_ref = dataset_ref.table("fct_user_engagement")
        schema = [
            bigquery.SchemaField("ds", "DATE"),
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField("action", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("restaurant_id", "INTEGER"),
            bigquery.SchemaField("user_id", "INTEGER"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ds",
        )
        table = bq_client.create_table(table)

        self.next(self.join)

    @step
    def join(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.
        """
        print("TableSetup is all done.")


if __name__ == "__main__":
    TableSetup()
