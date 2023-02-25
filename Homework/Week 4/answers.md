## Week 4 Homework 

### Question 1: 

**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)** 
- 41648442
- 51648442
- 61648442
- 71648442
You'll need to have completed the "Build the first dbt models" video and have been able to run the models via the CLI. 
You should find the views and models for querying in your DWH.

``` sql
-- Create yellow external table
CREATE OR REPLACE EXTERNAL TABLE stellarismusv2.trips.yellow_tripdata_ext
OPTIONS(
  format = 'parquet',
  uris = ['gs://raw-a43b47dc95/yellow/*']
);

-- Create yellow table
CREATE OR REPLACE TABLE stellarismusv2.trips.yellow_tripdata AS
SELECT * FROM stellarismusv2.trips.yellow_tripdata_ext;

-- Create external table (green data)
CREATE OR REPLACE EXTERNAL TABLE stellarismusv2.trips.green_tripdata_ext
OPTIONS(
  format = 'parquet',
  uris = ['gs://raw-a43b47dc95/green/*']
);

-- Create green table 
CREATE OR REPLACE TABLE stellarismusv2.trips.green_tripdata AS
SELECT * FROM stellarismusv2.trips.green_tripdata_ext;

-- from dbt
SELECT
EXTRACT(YEAR FROM pickup_datetime),
COUNT(pickup_datetime)
FROM `stellarismusv2.trips.fct_trips`
WHERE
EXTRACT(YEAR FROM pickup_datetime) = 2019 OR
EXTRACT(YEAR FROM pickup_datetime) = 2020
GROUP BY EXTRACT(YEAR FROM pickup_datetime)
LIMIT 10
``` 
Ans: 61,602,960

### Question 2: 

**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos**

You will need to complete "Visualising the data" videos, either using data studio or metabase. 

- 89.9/10.1
- 94/6
- 76.3/23.7
- 99.1/0.9

Ans: 89.8% / 10.2%


### Question 3: 

**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)**  

- 33244696
- 43244696
- 53244696
- 63244696


``` sql
-- Create external table from gcs data
CREATE OR REPLACE EXTERNAL TABLE stellarismusv2.trips.fhv_tripdata_ext
  format = 'parquet',
  uris = [gs://raw-a43b47dc95/fhv/**']
 
);

-- Create table from external table
CREATE OR REPLACE TABLE stellarismusv2.trips.fhv_tripdata AS
SELECT * FROM stellarismusv2.trips.fhv_tripdata_ext;

-- run dbt build --var "is_test_run: false"

SELECT COUNT(pickup_datetime)
FROM `stellarismusv2.trips.fhv_tripdata`
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019

```

Ans: 43244696

### Question 4: 

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)**  

Create a core model for the stg_fhv_tripdata joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.


- 12998722
- 22998722
- 32998722
- 42998722

Ans: 22998722

### Question 5: 

**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table**
Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

- March
- April
- January
- December


```sql 
SELECT
EXTRACT(MONTH FROM pickup_datetime) as month,
COUNT(EXTRACT(MONTH FROM pickup_datetime))
FROM `stellarismusv2.trips.fhv_tripdata`
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019
GROUP BY month
ORDER BY month ASC
```

Ans: January
