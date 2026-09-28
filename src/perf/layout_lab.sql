-- src/perf/layout_lab.sql — partitioning vs liquid clustering on one period of real data (Task 4.4)
-- Run in the SQL editor. For each SELECT open the query profile and record:
--   files read, files pruned, bytes read, duration.

-- A: partitioned by day (31 partitions)
CREATE OR REPLACE TABLE workspace.prd_ops.perf_partitioned
PARTITIONED BY (pickup_date)
AS SELECT * FROM workspace.prd_lakehouse.silver_trips WHERE period = '2025-01';

-- B: liquid clustering on the two columns the query filters on
CREATE OR REPLACE TABLE workspace.prd_ops.perf_clustered
CLUSTER BY (pickup_date, pu_zone_id)
AS SELECT * FROM workspace.prd_lakehouse.silver_trips WHERE period = '2025-01';
OPTIMIZE workspace.prd_ops.perf_clustered;

DESCRIBE DETAIL workspace.prd_ops.perf_partitioned;     -- record numFiles and sizeInBytes
DESCRIBE DETAIL workspace.prd_ops.perf_clustered;

-- The same selective query on both: one zone (132 = JFK Airport), one week.
SELECT count(*), avg(trip_miles) FROM workspace.prd_ops.perf_partitioned WHERE pu_zone_id = 132 AND pickup_date BETWEEN '2025-01-13' AND '2025-01-19';
SELECT count(*), avg(trip_miles) FROM workspace.prd_ops.perf_clustered   WHERE pu_zone_id = 132 AND pickup_date BETWEEN '2025-01-13' AND '2025-01-19';

-- Let predictive optimization choose clustering keys from real query patterns.
-- If this errors, predictive optimization is not enabled for the catalog: record that.
ALTER TABLE workspace.prd_ops.perf_clustered CLUSTER BY AUTO;
DESCRIBE TABLE EXTENDED workspace.prd_ops.perf_clustered;

-- Is predictive optimization on? Catalog Explorer -> workspace -> Details, or:
DESCRIBE CATALOG EXTENDED workspace;

-- Clean-up when the numbers are in the evidence pack:
-- DROP TABLE workspace.prd_ops.perf_partitioned;
-- DROP TABLE workspace.prd_ops.perf_clustered;
