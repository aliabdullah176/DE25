import numpy as np
import pandas as pd

import pyarrow.parquet as pq

data_path = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_2019-01.parquet"

data = pq.read_table(data_path)

data

## data is correct, something wrong must be happening in the airflow stuff