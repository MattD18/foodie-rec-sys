from metaflow import FlowSpec, step

import os

from google.cloud import bigquery


class PredictionStorageSetup(FlowSpec):
    """
    Flow to create prediction tables
    """

    @step
    def start(self):
        """
        This is the 'start' step. All flows must have a step named 'start' that
        is the first step in the flow.
        """
        self.next(self.create_tables)

    @step
    def create_tables(self):
        """
        A step for creating prediction tables

        Note:
        have to store restaurant predictions as string, need to revist
        """
        project_id = os.environ['GCP_PROJECT']
        # establish bigquery connection
        bq_client = bigquery.Client(project=project_id)

        dataset_ref = bigquery.DatasetReference(project_id, 'predictions')
        table_ref = dataset_ref.table("user_daily_recs")
        schema = [
            bigquery.SchemaField("ds", "DATE"),
            bigquery.SchemaField("user_id", "INTEGER"),
            bigquery.SchemaField("restaurant_list", "STRING"),
            bigquery.SchemaField("model_type", "STRING"),
            bigquery.SchemaField("model_id", "INTEGER"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ds",
        )

        table = bq_client.create_table(table)
        self.next(self.end)

    @step
    def end(self):
        """
        This is the 'end' step. All flows must have an 'end' step, which is the
        last step in the flow.
        """
        print("PredictionStorageSetup is all done.")


if __name__ == "__main__":
    PredictionStorageSetup()
