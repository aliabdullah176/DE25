import pandas as pd

import argparse

from time import time

from sqlalchemy import create_engine
import sqlalchemy


# read in a small sample of the data to see whats up
# df_sample = pd.read_csv(input_data_path, nrows=100)
# df_sample.head(1)
# df_sample.dtypes


# create the create statement and inspect
# issue 1: timestamps are strings as we saw in dtypes
# print(pd.io.sql.get_schema(df_sample, name='yellow_taxi_data', con=engine))

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    schema = params.schema
    table_name_zones = params.table_name_zones

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    with engine.connect() as conn:
        try:
            conn.execute(sqlalchemy.schema.CreateSchema(schema, if_not_exists=True))
            conn.commit()
        except:
            raise

    df_chunked = pd.read_csv(url, iterator=True, chunksize=100000)

    df_first = next(df_chunked)
    df_first.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace', schema=schema)

    df_chunked = pd.read_csv(url, iterator=True, chunksize=100000)

    for chunk in df_chunked:
        t_start = time()
        chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_pickup_datetime)
        chunk.tpep_dropoff_datetime = pd.to_datetime(chunk.tpep_dropoff_datetime)
        chunk.to_sql(name=table_name, con=engine, if_exists='append', schema=schema)
        t_end = time()

        print(f'inserted another chunk of shape {chunk.shape}, took %.3f second' % (t_end - t_start))

    sql_query = f"""SELECT COUNT(*) FROM {schema}.{table_name}"""
    print(pd.read_sql(sql_query, con=engine))


    df_zones = pd.read_csv("https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv")
    df_zones.to_sql(name=table_name_zones, con=engine, if_exists='replace', schema=schema)


# nice, this is what I had in mind when starting this weeks module
# other things to do. 
# replace postgres with sqlite? I have had good experiences with sqlite. but probably good to learn postgres as well
# how does something like this connect with BQ? is it seamless?



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')
    parser.add_argument('--schema', required=True, help='schema name for postgres')
    parser.add_argument('--table_name_zones', required=True, help='name of table that ')

    args = parser.parse_args()

    main(args)