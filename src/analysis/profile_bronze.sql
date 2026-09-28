-- src/analysis/profile_bronze.sql — Task 2.2: profile Bronze BEFORE writing or tuning a single rule.
-- Run on dev (the development slice). Paste the numbers that justify each threshold into README §6.
USE CATALOG workspace;
USE SCHEMA dev_lakehouse;

-- 1. Row counts per file, and each file's own count straight from the Volume (they must match)
SELECT _source_file, count(*) AS bronze_rows FROM bronze_hvfhv_trips GROUP BY ALL ORDER BY 1;
SELECT _source_file, count(*) AS bronze_rows FROM bronze_yellow_trips GROUP BY ALL ORDER BY 1;
SELECT count(*) AS file_rows
FROM read_files('/Volumes/workspace/dev_landing/raw/trips/fhvhv_tripdata_2024-11.parquet', format => 'parquet');

-- 2. Types as delivered: expect TIMESTAMP_NTZ for the trip timestamps (wall-clock, no zone)
DESCRIBE TABLE bronze_hvfhv_trips;

-- 3. HVFHV: null rates, platforms, and the ranges the drop rules act on
SELECT
  count(*)                                                     AS rows,
  round(100 * avg(CASE WHEN request_datetime IS NULL THEN 1 ELSE 0 END), 3)  AS pct_null_request,
  round(100 * avg(CASE WHEN on_scene_datetime IS NULL THEN 1 ELSE 0 END), 3) AS pct_null_on_scene,
  round(100 * avg(CASE WHEN trip_time < 60 THEN 1 ELSE 0 END), 3)            AS pct_under_60s,
  round(100 * avg(CASE WHEN trip_time > 21600 THEN 1 ELSE 0 END), 4)         AS pct_over_6h,
  round(100 * avg(CASE WHEN trip_miles > 200 THEN 1 ELSE 0 END), 4)          AS pct_over_200mi,
  round(100 * avg(CASE WHEN dropoff_datetime <= pickup_datetime THEN 1 ELSE 0 END), 4) AS pct_dropoff_not_after_pickup,
  round(100 * avg(CASE WHEN base_passenger_fare < 0 THEN 1 ELSE 0 END), 4)   AS pct_negative_fare,
  round(100 * avg(CASE WHEN pickup_datetime < request_datetime THEN 1 ELSE 0 END), 4) AS pct_negative_wait,
  round(100 * avg(CASE WHEN PULocationID IN (264, 265) OR DOLocationID IN (264, 265) THEN 1 ELSE 0 END), 3) AS pct_unknown_zone
FROM bronze_hvfhv_trips;

SELECT hvfhs_license_num, count(*) AS trips FROM bronze_hvfhv_trips GROUP BY ALL ORDER BY trips DESC;

SELECT percentile_approx(trip_time, array(0.001, 0.01, 0.5, 0.99, 0.9999)) AS trip_time_s_quantiles,
       percentile_approx(trip_miles, array(0.001, 0.5, 0.99, 0.9999))     AS trip_miles_quantiles
FROM bronze_hvfhv_trips;

-- 4. Yellow: the problems you met in Assignment 1
SELECT
  count(*)                                                                    AS rows,
  round(100 * avg(CASE WHEN fare_amount < 0 THEN 1 ELSE 0 END), 3)            AS pct_negative_fare,
  round(100 * avg(CASE WHEN trip_distance = 0 THEN 1 ELSE 0 END), 3)          AS pct_zero_distance,
  round(100 * avg(CASE WHEN timestampdiff(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) < 60 THEN 1 ELSE 0 END), 3) AS pct_under_60s,
  round(100 * avg(CASE WHEN PULocationID IN (264, 265) THEN 1 ELSE 0 END), 3) AS pct_unknown_pu_zone,
  round(100 * avg(CASE WHEN passenger_count IS NULL THEN 1 ELSE 0 END), 3)    AS pct_null_passengers
FROM bronze_yellow_trips;

-- 5. Trips dated outside their file's month (decide: file or timestamp defines the period?)
SELECT _source_file, date_format(pickup_datetime, 'yyyy-MM') AS pickup_month, count(*) AS trips
FROM bronze_hvfhv_trips GROUP BY ALL HAVING pickup_month <> substr(_source_file, 16, 7) ORDER BY 1, 2;
SELECT _source_file, date_format(tpep_pickup_datetime, 'yyyy-MM') AS pickup_month, count(*) AS trips
FROM bronze_yellow_trips GROUP BY ALL HAVING pickup_month <> substr(_source_file, 17, 7) ORDER BY 1, 2;

-- 6. Candidate duplicates on the business key (before Silver) — HVFHV has no trip id
SELECT count(*) - count(DISTINCT hvfhs_license_num, pickup_datetime, dropoff_datetime, PULocationID,
                              DOLocationID, trip_miles, base_passenger_fare) AS duplicate_rows
FROM bronze_hvfhv_trips;
