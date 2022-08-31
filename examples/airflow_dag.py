import os

import pendulum
from airflow import DAG
from airflow.decorators import task
from looker_ingestion import NoDataException, sync_data

dir_path = os.path.dirname(os.path.realpath(__file__))

with DAG(
    dag_id="example_dag",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:

    @task(task_id="looker_to_s3")
    def looker_to_s3():
        """
        looker_ingestion will check to see if we have the latest data
        and then keep running until we are fully caught up
        """
        backfill = True
        while backfill:
            try:
                sync_data.extract_data(
                    json_filename=f"{dir_path}/my_config.json",
                    aws_storage_bucket_name="my_s3_bucket",
                )
            except NoDataException:
                backfill = False
