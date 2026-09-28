-- src/pipeline/gold.sql — consumer-facing objects, written to the gold schema by fully qualified name.
-- Every Gold object is built from Silver (via the temporary view v_trip_segments), never from another
-- governed Gold object: a pipeline refresh evaluates policies as the pipeline owner, so reading a
-- masked Gold object would bake masked values into the next one.
--
-- Why materialized views: each object is an aggregate or join that must reflect every change in its
-- sources (late files, de-duplication, the zone list). A streaming table would process each input row
-- once and never revisit it; a view would recompute on every dashboard query.

-- ---------------------------------------------------------------------------------------------
-- 1. Which pickup zones count as inside the Congestion Relief Zone — derived from the data itself.
--    A zone is "in" when at least 90% of its pickups from 6 January 2025 carry a congestion fee,
--    and it has at least 200 such pickups (so a zone with 3 trips, all to Midtown, cannot qualify).
-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_crz_zones
COMMENT 'Pickup zones treated as inside the Congestion Relief Zone, derived from 2025 fee incidence'
AS SELECT
  z.zone_id, z.zone, z.borough,
  count(*)                                                        AS pickups_2025,
  avg(CASE WHEN t.cbd_congestion_fee > 0 THEN 1.0 ELSE 0.0 END)   AS fee_share,
  count(*) >= 200
    AND avg(CASE WHEN t.cbd_congestion_fee > 0 THEN 1.0 ELSE 0.0 END) >= 0.90 AS in_crz
FROM silver_trips t
JOIN dim_zone z ON t.pu_zone_id = z.zone_id
WHERE t.pickup_date BETWEEN DATE'2025-01-06' AND DATE'2025-02-28'
GROUP BY z.zone_id, z.zone, z.borough;

-- ---------------------------------------------------------------------------------------------
-- 2. The governed object: row filter and masks are part of the definition, so every refresh keeps them.
--    Day 5 does NOT tag this one: a column cannot carry both a manual mask and an ABAC mask.
-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_platform_daily (
  service_date        DATE,
  platform_code       STRING,
  platform            STRING,
  crz_segment         STRING,
  fee_era             STRING,
  trips               BIGINT,
  avg_passenger_fare  DOUBLE,
  avg_wait_min        DOUBLE,
  total_driver_pay    DOUBLE MASK ${a2.ops}.mask_money,
  avg_driver_share    DOUBLE MASK ${a2.ops}.mask_money
)
WITH ROW FILTER ${a2.ops}.rf_scope ON (platform_code)
CLUSTER BY (service_date)
COMMENT 'Daily HVFHV KPIs by platform. Rows filtered by platform entitlement, driver pay masked.'
AS SELECT
  pickup_date                                               AS service_date,
  platform_code,
  platform,
  crz_segment,
  fee_era,
  count(*)                                                  AS trips,
  avg(passenger_fare)                                       AS avg_passenger_fare,
  avg(CASE WHEN wait_min >= 0 THEN wait_min END)            AS avg_wait_min,
  sum(driver_pay)                                           AS total_driver_pay,
  sum(driver_pay) / nullif(sum(passenger_fare), 0)          AS avg_driver_share
FROM v_trip_segments
WHERE service = 'hvfhv'
GROUP BY ALL;

-- ---------------------------------------------------------------------------------------------
-- 3. BQ1 + BQ2: daily trips by service, platform and segment. Additive measures only, so any
--    rollup (per month, per era, per service) is a plain SUM. total_driver_pay is tagged on Day 5.
-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_daily_segment
CLUSTER BY (service_date)
COMMENT 'Daily trips, fares, driver pay and congestion fees by service, platform and zone segment'
AS SELECT
  pickup_date                                               AS service_date,
  period,
  fee_era,
  service,
  platform_code,
  platform,
  crz_segment,
  count(*)                                                  AS trips,
  sum(passenger_fare)                                       AS total_passenger_fare,
  avg(passenger_fare)                                       AS avg_passenger_fare,
  sum(driver_pay)                                           AS total_driver_pay,
  count_if(cbd_congestion_fee > 0)                          AS trips_with_fee,
  sum(cbd_congestion_fee)                                   AS total_cbd_fee
FROM v_trip_segments
GROUP BY ALL;

-- ---------------------------------------------------------------------------------------------
-- 4. BQ2: who pays the toll, and driver share, per month and fee era.
--    fee_era is part of the grain because January holds both eras (1-4 Jan pre-toll, 5-31 toll).
--    Driver share = sum(driver_pay) / sum(passenger_fare), where passenger_fare is the BASE fare:
--    no tolls, taxes, surcharges, tips or the congestion fee (a pass-through, not revenue).
--    total_driver_pay and avg_driver_share are tagged on Day 5.
-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_fee_summary
COMMENT 'Fee incidence, fares and driver share by month, fee era, platform and zone segment'
AS SELECT
  period,
  fee_era,
  service,
  platform_code,
  platform,
  crz_segment,
  count(*)                                                  AS trips,
  count_if(cbd_congestion_fee > 0)                          AS trips_with_fee,
  avg(CASE WHEN cbd_congestion_fee > 0 THEN 1.0 ELSE 0.0 END) AS share_with_fee,
  sum(cbd_congestion_fee)                                   AS total_cbd_fee,
  avg(CASE WHEN cbd_congestion_fee > 0 THEN cbd_congestion_fee END) AS avg_fee_when_charged,
  sum(passenger_fare)                                       AS total_passenger_fare,
  avg(passenger_fare)                                       AS avg_passenger_fare,
  sum(driver_pay)                                           AS total_driver_pay,
  sum(driver_pay) / nullif(sum(passenger_fare), 0)          AS avg_driver_share
FROM v_trip_segments
GROUP BY ALL;

-- ---------------------------------------------------------------------------------------------
-- 5. BQ3 (daily trend): wait = pickup - request, HVFHV only. Negative waits are excluded from the
--    percentiles and counted, not silently dropped.
-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_wait_times
CLUSTER BY (service_date)
COMMENT 'Daily p50 / p90 wait (minutes) by hour, platform and zone segment; HVFHV only'
AS SELECT
  pickup_date                                               AS service_date,
  pickup_hour,
  fee_era,
  platform_code,
  platform,
  crz_segment,
  count(*)                                                  AS trips,
  count_if(wait_min >= 0)                                   AS trips_with_valid_wait,
  count_if(wait_min < 0)                                    AS negative_waits_excluded,
  percentile_approx(CASE WHEN wait_min >= 0 THEN wait_min END, 0.5) AS p50_wait_min,
  percentile_approx(CASE WHEN wait_min >= 0 THEN wait_min END, 0.9) AS p90_wait_min
FROM v_trip_segments
WHERE service = 'hvfhv'
GROUP BY ALL;

-- ---------------------------------------------------------------------------------------------
-- 6. BQ3 (the answer): percentiles over each WHOLE fee era. Percentiles are not additive: the
--    average of 60 daily p90s is not the p90 of those 60 days. So the before/after numbers come from
--    here, computed once over all the trips in each era.
--    GROUPING SETS gives two grains in one object: per hour, and all hours together (hour_scope).
-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_wait_era
COMMENT 'p50 / p90 wait (minutes) per fee era, by hour and for all hours; HVFHV only'
AS SELECT
  fee_era,
  CASE WHEN grouping(pickup_hour) = 1 THEN 'all_hours' ELSE 'by_hour' END AS hour_scope,
  pickup_hour,
  platform_code,
  platform,
  crz_segment,
  count(*)                                                  AS trips,
  count_if(wait_min >= 0)                                   AS trips_with_valid_wait,
  count_if(wait_min < 0)                                    AS negative_waits_excluded,
  percentile_approx(CASE WHEN wait_min >= 0 THEN wait_min END, 0.5) AS p50_wait_min,
  percentile_approx(CASE WHEN wait_min >= 0 THEN wait_min END, 0.9) AS p90_wait_min
FROM v_trip_segments
WHERE service = 'hvfhv'
GROUP BY GROUPING SETS (
  (fee_era, pickup_hour, platform_code, platform, crz_segment),
  (fee_era, platform_code, platform, crz_segment)
);

-- ---------------------------------------------------------------------------------------------
-- 7. BQ4: daily trips by service joined to Central Park weather (GHCND station USW00094728).
--    Classes: snow (snow > 0), wet (prcp >= 5 mm), dry (everything else); no_data if the day is missing.
-- ---------------------------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_weather_demand
COMMENT 'Daily trips by service with Central Park precipitation, snow and temperature'
AS WITH daily AS (
  SELECT pickup_date AS service_date, period, fee_era, service, count(*) AS trips
  FROM v_trip_segments
  GROUP BY pickup_date, period, fee_era, service
)
SELECT
  d.service_date,
  d.period,
  d.fee_era,
  d.service,
  d.trips,
  w.prcp                                                    AS prcp_mm,
  w.snow                                                    AS snow_mm,
  w.tmax                                                    AS tmax_c,
  w.tmin                                                    AS tmin_c,
  CASE WHEN w.snow > 0     THEN 'snow'
       WHEN w.prcp >= 5    THEN 'wet'
       WHEN w.prcp IS NULL THEN 'no_data'
       ELSE 'dry' END                                       AS weather_class
FROM daily d
LEFT JOIN silver_weather_daily w
  ON w.obs_date = d.service_date AND w.station_id = 'USW00094728';
