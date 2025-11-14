-- this was a good read: https://medium.com/data-science/burn-data-rather-than-money-with-bigquery-the-definitive-guide-1b50a9fdf096
-- homework questions: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/03-data-warehouse/homework.md

-- 0. Upload data to GCS bucket
-- For this homework we will be using the Yellow Taxi Trip Records for January 2024 - June 2024
-- Created using airflow orchestration

-- 1. Create an external table using the Yellow Taxi Trip Records.

-- creates an external table ie references the parquet files in gcs bucket
CREATE OR REPLACE EXTERNAL TABLE `de25.ny_taxi_external`
OPTIONS (
  FORMAT = "PARQUET",
  URIs = ["gs://de25/raw/trip-data/*.parquet"]
);

-- 2. Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table). 
CREATE OR REPLACE TABLE `de25.ny_taxi`
AS (
  SELECT * FROM  `de25.ny_taxi_external`
);

-- Question 1: What is count of records for the 2024 Yellow Taxi Data?
-- 20,332,093 (can check from the details page for the table)
-- or do simple query
SELECT COUNT(*)
FROM `de25.ny_taxi`; -- doesn't cost any money either since table (and metadata) is already created

-- Q2. Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT COUNT (DISTINCT PULocationID) FROM `de25.ny_taxi`; --  This query will process 155.12 MB when run.
SELECT COUNT (DISTINCT PULocationID) FROM `de25.ny_taxi_external`; -- This query will process 0 B when run.

-- Q3. Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. 
-- Now write a query to retrieve the PULocationID and DOLocationID on the same table. 
-- Why are the estimated number of Bytes different?
SELECT PULocationID FROM `de25.ny_taxi`; -- 155MB
SELECT PULocationID, DOLocationID FROM `de25.ny_taxi`; -- 310MB

-- Different because columnar storage. reading two column so ~twice the data scanned

-- Q4. How many records have a fare_amount of 0?
SELECT count(*) from `de25.ny_taxi` where fare_amount = 0;
-- 8333

-- Q5 What is the best strategy to make an optimized table in Big Query if your query will:
-- always filter based on tpep_dropoff_datetime and 
-- order the results by VendorID 
-- (Create a new table with this strategy)

-- A: Partition by tpep_dropoff_datetime and Cluster on VendorID

CREATE OR REPLACE TABLE `de25.ny_taxi_part_clust`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS (
  SELECT * FROM `de25.ny_taxi`
); -- 2.7 GB processed

-- Q6 Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

-- Use the materialized table you created earlier in your from clause and note the estimated bytes. 
-- Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

-- Choose the answer which most closely matches.

--     12.47 MB for non-partitioned table and 326.42 MB for the partitioned table
--     310.24 MB for non-partitioned table and 26.84 MB for the partitioned table ----<<<<< answer
--     5.87 MB for non-partitioned table and 0 MB for the partitioned table
--     310.31 MB for non-partitioned table and 285.64 MB for the partitioned table

SELECT DISTINCT VendorID
FROM `de25.ny_taxi` -- 310 MB
-- FROM `de25.ny_taxi_part_clust` -- 26.84 MB wow
WHERE tpep_dropoff_datetime between "2024-03-01" and "2024-03-15";

-- Q7. Where is the data stored in the External Table you created?

--     Big Query
--     Container Registry
--     GCP Bucket  ----<<<<< answer
--     Big Table

-- Q8. It is best practice in Big Query to always cluster your data:
--     True
--     False ----<<<<< answer. Small tables probably don't benefit from clustering

-- Q9. (Bonus: Not worth points) Question 9:
-- No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

-- Answered above. 0 Bytes because metadata is already created
