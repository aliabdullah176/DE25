import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# dataset_file format -> """yellow_tripdata_{yyyy}-{mm}.csv"""
dataset_file = """yellow_tripdata_year-month.csv"""

dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

# plan
# we need to load 2 years of data -> https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-01.parquet
# we also need to load the zone mapping file -> https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
# we also need to load the for hire data for just 2019 -> https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-01.parquet

# lets bring in the useful functions from the coursework
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB
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
    "depends_on_past": False,
    "retries": 1,
}

# one thing from the course is that dag can actually be passed as a parameter to a function
# so like the code that we want to use to downlaod and then upload data is boilerplate
# args would be the data path, format and then start end date, and the bigquery table

# DAG constructor already has a useful start and end date parameter. We can take advantage of that
# lets build out this function with that in mind later
def upload_file_as_bigquery_table(
    dag: DAG, 
    url_template:str,
): 
    print("nothing")

# lets build out the paths first
BASE_URL = """https://d37ci6vzurychx.cloudfront.net/"""
TRIPS_DATA_URL = BASE_URL + """trip-data/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"""
TRIPS_DATA_LOCAL_PATH = AIRFLOW_HOME + """trip-data/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"""
TRIPS_DATA_GCS_PATH = BUCKET + """\trip-data/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"""

yellow_taxi_DAG = DAG(
    dag_id="yellow_taxi_data",
    schedule="@monthly",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 12, 31), # does this include both dates or will it exclude any?
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de'],
)
    delete_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"rm -rf {TRIPS_DATA_LOCAL_PATH}"        
    )

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {TRIPS_DATA_URL} > {TRIPS_DATA_LOCAL_PATH}"        
    )

    # optionally we need a conditional operator that runs the parquet converter if file is a csv

    delete_dataset_task >> download_dataset_task