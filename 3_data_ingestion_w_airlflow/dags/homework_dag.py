import os
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

YELLOW_TAXI_SCHEMA = pa.schema([
    ('VendorID', pa.float64()),
    ('tpep_pickup_datetime', pa.timestamp('us')),
    ('tpep_dropoff_datetime', pa.timestamp('us')),
    ('passenger_count', pa.float64()),
    ('trip_distance', pa.float64()),
    ('RatecodeID', pa.float64()),
    ('store_and_fwd_flag', pa.string()),
    ('PULocationID', pa.float64()),
    ('DOLocationID', pa.float64()),
    ('payment_type', pa.float64()),
    ('fare_amount', pa.float64()),
    ('extra', pa.float64()),
    ('mta_tax', pa.float64()),
    ('tip_amount', pa.float64()),
    ('tolls_amount', pa.float64()),
    ('improvement_surcharge', pa.float64()),
    ('total_amount', pa.float64()),
    ('congestion_surcharge', pa.float64()),
    ('airport_fee', pa.float64()),
])

GREEN_TAXI_SCHEMA = pa.schema([
    ('VendorID', pa.float64()),
    ('lpep_pickup_datetime', pa.timestamp('us')),
    ('lpep_dropoff_datetime', pa.timestamp('us')),
    ('store_and_fwd_flag', pa.string()),
    ('RatecodeID', pa.float64()),
    ('PULocationID', pa.float64()),
    ('DOLocationID', pa.float64()),
    ('passenger_count', pa.float64()),
    ('trip_distance', pa.float64()),
    ('fare_amount', pa.float64()),
    ('extra', pa.float64()),
    ('mta_tax', pa.float64()),
    ('tip_amount', pa.float64()),
    ('tolls_amount', pa.float64()),
    ('ehail_fee', pa.float64()),
    ('improvement_surcharge', pa.float64()),
    ('total_amount', pa.float64()),
    ('payment_type', pa.float64()),
    ('trip_type', pa.float64()),
    ('congestion_surcharge', pa.float64()),
])

FHV_TAXI_SCHEMA = pa.schema([
    ('dispatching_base_num', pa.string()),
    ('pickup_datetime', pa.timestamp('us')),
    ('dropOff_datetime', pa.timestamp('us')),
    ('PULocationID', pa.float64()),
    ('DOLocationID', pa.float64()),
    ('SR_Flag', pa.float64()),
    ('Affiliated_base_number', pa.string()),
])
	


# dataset_file format -> """yellow_tripdata_{yyyy}-{mm}.csv"""
dataset_file = """yellow_tripdata_year-month.csv"""

dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
BIGQUERY_DATASET = os.environ.get("GCP_BQ_DATASET", 'de25')

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

def create_delete_file_bash_operator(path:str, task_id:str):
    return BashOperator(
            task_id=task_id,
            bash_command=f"rm -rf {path} || true"
        )

def fix_parquet_schema(local_file: str, schema: pa.Schema = None):
    """
    Read parquet file and rewrite with the provided schema
    
    Args:
        local_file: Path to the parquet file
        schema: PyArrow schema to apply to the file
    """
    if schema:
        # Read the parquet file
        table = pq.read_table(local_file)
        
        target_schema = schema
        
        # Cast all columns to match target schema
        casted_arrays = []
        for field in target_schema:
            if field.name in table.column_names:
                # Cast existing column to target type
                col = table.column(field.name).cast(field.type)
                casted_arrays.append(col)
            else:
                # Create null column if field doesn't exist
                null_array = pa.array([None] * len(table), type=field.type)
                casted_arrays.append(null_array)
        
        # Create new table with fixed schema
        new_table = pa.Table.from_arrays(casted_arrays, schema=target_schema)
        
        # Write back to the same file
        pq.write_table(new_table, local_file)

# one thing from the course is that dag can actually be passed as a parameter to a function
# so like the code that we want to use to downlaod and then upload data is boilerplate
# args would be the data path, format and then start end date, and the bigquery table

# DAG constructor already has a useful start and end date parameter. We can take advantage of that
# lets build out this function with that in mind
def upload_file_as_bigquery_table(
    dag: DAG, 
    remote_url:str,
    local_path:str,
    gcs_path:str,
    schema:pa.Schema = None,
    # bq_table_name:str,
    # file_type:str=".parquet"
): 
    with dag:
        delete_dataset_task = create_delete_file_bash_operator(path=local_path, task_id="delete_dataset_task")

        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"mkdir -p $(dirname {local_path}) && curl -sSLf {remote_url} > {local_path}"
        )

        fix_schema_task = PythonOperator(
            task_id="fix_parquet_schema_task",
            python_callable=fix_parquet_schema,
            op_kwargs={
                "local_file": local_path,
                "schema": schema,
            }
        )

        upload_to_gcs_task = PythonOperator(
            task_id = "upload_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": f"raw/{gcs_path}",
                "local_file": f"{local_path}",
            }
        )

        another_delete_dataset_task = create_delete_file_bash_operator(path=local_path, task_id="another_delete_dataset_task")

        delete_dataset_task >> download_dataset_task >> fix_schema_task >> upload_to_gcs_task >> another_delete_dataset_task

    logging.info("upload_file_as_bigquery_table(): done")

# paths for yellow taxi data
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/"
TRIPS_DATA_URL = BASE_URL + "trip-data/yellow_tripdata_{{ logical_date.strftime('%Y-%m') }}.parquet"
TRIPS_DATA_LOCAL_PATH = AIRFLOW_HOME + "/trip-data/yellow_tripdata_{{ logical_date.strftime('%Y-%m') }}.parquet"
TRIPS_DATA_GCS_PATH = "trip-data/yellow_tripdata_{{ logical_date.strftime('%Y-%m') }}.parquet"


yellow_taxi_DAG = DAG(
    dag_id="yellow_taxi_data",
    schedule="@monthly",
    start_date=datetime(2018, 12, 31),
    end_date=datetime(2020, 12, 31),
    default_args=default_args,
    catchup=True,
    max_active_runs=6,
    tags=['dtc-de'],
)

upload_file_as_bigquery_table(    
    dag=yellow_taxi_DAG, 
    remote_url=TRIPS_DATA_URL,
    local_path=TRIPS_DATA_LOCAL_PATH,
    gcs_path=TRIPS_DATA_GCS_PATH,
    schema=YELLOW_TAXI_SCHEMA,
    # bq_table_name="yellow_taxi_trips",
    # file_type="parquet",
    )

# this works
# lets repeat for the other tables now

green_tax_DAG = DAG(
    dag_id="green_taxi_data",
    schedule="@monthly",
    start_date=datetime(2018, 12, 31),
    end_date=datetime(2020, 12, 31),
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['dtc-de'],
)

# https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-03.parquet

GT_TRIPS_DATA_URL = BASE_URL + "trip-data/green_tripdata_{{ logical_date.strftime('%Y-%m') }}.parquet"
GT_TRIPS_DATA_LOCAL_PATH = AIRFLOW_HOME + "/green-trip-data/fhv_tripdata_{{ logical_date.strftime('%Y-%m') }}.parquet"
GT_TRIPS_DATA_GCS_PATH = "green-trip-data/green_tripdata_{{ logical_date.strftime('%Y-%m') }}.parquet"

upload_file_as_bigquery_table(    
    dag=green_tax_DAG, 
    remote_url=GT_TRIPS_DATA_URL,
    local_path=GT_TRIPS_DATA_LOCAL_PATH,
    gcs_path=GT_TRIPS_DATA_GCS_PATH,
    schema=GREEN_TAXI_SCHEMA
)


fhv_taxi_DAG = DAG(
    dag_id="fhv_taxi_data",
    schedule="@monthly",
    start_date=datetime(2018, 12, 31),
    end_date=datetime(2019, 12, 31),
    default_args=default_args,
    catchup=False,
    max_active_runs=3,
    tags=['dtc-de'],
)


# https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-01.parquet
FHV_TRIPS_DATA_URL = BASE_URL + "trip-data/fhv_tripdata_{{ logical_date.strftime('%Y-%m') }}.parquet"
FHV_TRIPS_DATA_LOCAL_PATH = AIRFLOW_HOME + "/fhv-trip-data/fhv_tripdata_{{ logical_date.strftime('%Y-%m') }}.parquet"
FHV_TRIPS_DATA_GCS_PATH = "fhv-trip-data/fhv_tripdata_{{ logical_date.strftime('%Y-%m') }}.parquet"
# probably should functionize creation of these paths as well but not key for this excercise really

upload_file_as_bigquery_table(    
    dag=fhv_taxi_DAG, 
    remote_url=FHV_TRIPS_DATA_URL,
    local_path=FHV_TRIPS_DATA_LOCAL_PATH,
    gcs_path=FHV_TRIPS_DATA_GCS_PATH,
    schema=FHV_TAXI_SCHEMA,
    )

# finally the taxi zones

taxi_zones_DAG = DAG(
    dag_id="taxi_zones_data",
    schedule="@once",
    start_date=datetime(2019, 1, 1),
    default_args=default_args,
    catchup=False,
    tags=['dtc-de'],
)

# https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
ZONES_DATA_URL = BASE_URL + "misc/taxi_zone_lookup.csv"
ZONES_TRIPS_DATA_LOCAL_PATH = AIRFLOW_HOME + "/misc/taxi_zone_lookup.csv"
ZONES_TRIPS_DATA_GCS_PATH = "misc/taxi_zone_lookup.csv"

upload_file_as_bigquery_table(    
    dag=taxi_zones_DAG, 
    remote_url=ZONES_DATA_URL,
    local_path=ZONES_TRIPS_DATA_LOCAL_PATH,
    gcs_path=ZONES_TRIPS_DATA_GCS_PATH,
    # bq_table_name="taxi_zones",
    # file_type="csv",
    )


####
# saving this here for now for future reference
# this wouldnt work in the current design
# we would need a DAG to truncate the tables if exist
# then insert outside the main dag. cause otherwise it will just override every month
# its not required for the homework so skipping for now 

# the high level plan would be to have a one time DAG that creates the datasets and tables with the right schema if they dont exist
# then change the code below to be insert or upsert
# use partitioned tables to ease query time

# upload_as_bigquery_table_task = BigQueryInsertJobOperator(
#     task_id="upload_as_bigquery_table_task",
#     configuration={
#         "query": {
#             "query": f""" 
#                 LOAD DATA OVERWRITE {PROJECT_ID}.{BIGQUERY_DATASET}.{bq_table_name}
#                 FROM FILES (
#                 format = '{file_type.upper()}',
#                 uris = ['gs://{BUCKET}/raw/{gcs_path}']
#                 );
#             """,
#             "useLegacySql": False,
#         }
#     },
#     location='US',
# )