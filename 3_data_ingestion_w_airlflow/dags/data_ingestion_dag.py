import os
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import datetime, timedelta

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_file = "yellow_tripdata_2021-01.csv.gz"
dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv.gz', '.parquet')
BIGQUERY_DATASET = os.environ.get("GCP_BQ_DATASET", 'de25')


def format_to_parquet(src_file):
    if not src_file.endswith('.csv.gz'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv.gz', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date":datetime(2021, 1, 1),
    "catchup":False,
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_dag",
    schedule="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    delete_dataset_task = BashOperator(
        task_id="delete_dataset_task",
        bash_command=f"rm -rf {path_to_local_home}/{dataset_file}"
    )

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    bigquery_external_table_task = BigQueryInsertJobOperator(
        task_id="bigquery_external_table_task",
        configuration={
            "query": {
                "query": f"""
                #     CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.external_table`
                #     OPTIONS (
                #         format = 'PARQUET',
                #         uris = ['gs://{BUCKET}/raw/{parquet_file}']
                #     );
                # """,
                "query": f""" 
                    LOAD DATA OVERWRITE {PROJECT_ID}.{BIGQUERY_DATASET}.external_table
                    FROM FILES (
                    format = 'PARQUET',
                    uris = ['gs://{BUCKET}/raw/{parquet_file}']
                    );
                """,
                "useLegacySql": False,
            }
        },
        location='US',  # Change to your dataset location if different (e.g., 'EU', 'us-central1')
    )

    delete_dataset_task >> download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task