-- BUILD TABLE
CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp.fhv_tripdata` OPTIONS (
        format = 'CSV',
        uris = ['gs://taxi-rides-ny/fhv_tripdata_2019-*.csv']
    );
-- 1 - What is count for fhv vehicle records for year 2019?
SELECT count(*)
FROM `dezoomcamp.fhv_tripdata`;
-- 2 - What is the estimated amount of data that will be read when you execute your query on the External Table and the Materialized Table?
SELECT COUNT(DISTINCT(dispatching_base_num))
FROM `dezoomcamp.fhv_tripdata`;
-- 3 - How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
SELECT COUNT(*)
FROM `dezoomcamp.fhv_tripdata`
where DOlocationID is null
    and PUlocationID is null;
-- 4 - What is the best strategy to make an optimized table in Big Query if your query will always filter by pickup_datetime and order by affiliated_base_number?
-- ANS: Partition by pickup_datetime Cluster on affiliated_base_number
-- 5 - Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019
--     Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed
CREATE OR REPLACE TABLE `dezoomcamp.fhv_nonpartitioned_tripdata` AS
SELECT *
FROM `dezoomcamp.fhv_tripdata`;
CREATE OR REPLACE TABLE `dezoomcamp.fhv_partitioned_tripdata` PARTITION BY DATE(dropoff_datetime) CLUSTER BY dispatching_base_num AS (
        SELECT *
        FROM `dezoomcamp.fhv_tripdata`
    );
SELECT count(*)
FROM `dezoomcamp.fhv_nonpartitioned_tripdata`
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
    AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');
SELECT count(*)
FROM `dezoomcamp.fhv_partitioned_tripdata`
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
    AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');
-- 6 - Where is the data stored in the External Table you created? GCP Bucket
-- 7 - It is best practice in Big Query to always cluster your data.: FALSE