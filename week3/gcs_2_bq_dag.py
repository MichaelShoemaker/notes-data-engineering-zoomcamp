#Week 2 homework
import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago


from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcs_2_bq_dag",
    schedule_interval="0 6 2 * *",
    default_args=default_args,
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020,1,1),
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de']
) as dag:

    gcs_2_gcs_task = GCSToGCSOperator(
    task_id="gcs_2_gcs_task",
    source_bucket=BUCKET,
    source_object='raw/yellow_tripdata*.parquet',
    destination_bucket=BUCKET,
    destination_object="yellow/",
    move_object=False
)




    gcs_2_bq_ext_task = BigQueryCreateExternalTableOperator(
        task_id="gcs_2_bq_ext_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": 'trips_data_all',
                "tableId": "external_yellow_tripdata",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/yellow/*"],
            },
        },

    )

    CREATE_PART_TABLE_QUERY=f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.yellow_tripdata_partitioned\
    PARTITION BY DATE(tpep_pickup_datetime) AS \
    SELECT * FROM {BIGQUERY_DATASET}.external_yellow_tripdata;"

    bg_ext_2_part_task=BigQueryInsertJobOperator(    
        task_id="bg_ext_2_part_task",
        configuration={
            "query": {
                "query": CREATE_PART_TABLE_QUERY,
                "useLegacySql": False,
        }
    },
)

    

gcs_2_gcs_task >> gcs_2_bq_ext_task >> bg_ext_2_part_task
