-- src/analysis/business_questions.sql — BQ1 to BQ4 against the prd gold schema (Section 7).
-- Run in the SQL editor on the Serverless Starter Warehouse, with full entitlement ('*', true).
-- Paste each result and your 2-3 plain-language sentences into docs/business-answers.md.
USE CATALOG workspace;
USE SCHEMA prd_gold;

-- =============================================================================================
-- BQ1. Did trips touching the zone fall after 5 January?
-- Definitions (state them in the answer):
--   segment : touches_zone = pickup OR dropoff in a zone from gold_crz_zones (in_crz); else outside
--   before  : 2024-11-01 .. 2024-12-31
--   after   : 2025-01-05 .. 2025-02-28   (1-4 January are pre-toll, so they are in neither window)
--   measure : average trips per calendar day
-- =============================================================================================
WITH daily AS (
  SELECT service_date, service, crz_segment, sum(trips) AS trips
  FROM gold_daily_segment GROUP BY ALL
), win AS (
  SELECT *, CASE WHEN service_date BETWEEN DATE'2024-11-01' AND DATE'2024-12-31' THEN 'before'
                 WHEN service_date BETWEEN DATE'2025-01-05' AND DATE'2025-02-28' THEN 'after' END AS win
  FROM daily
)
SELECT service, crz_segment,
       round(avg(CASE WHEN win = 'before' THEN trips END))                   AS trips_per_day_before,
       round(avg(CASE WHEN win = 'after'  THEN trips END))                   AS trips_per_day_after,
       round(100 * (avg(CASE WHEN win = 'after' THEN trips END)
                  / avg(CASE WHEN win = 'before' THEN trips END) - 1), 1)    AS pct_change,
       count(DISTINCT CASE WHEN win = 'before' THEN service_date END)        AS days_before,
       count(DISTINCT CASE WHEN win = 'after'  THEN service_date END)        AS days_after
FROM win WHERE win IS NOT NULL
GROUP BY ALL ORDER BY service, crz_segment;

-- BQ1 gap: (touches_zone % change) - (outside % change), per service. This is the number that
-- points at the zone: holidays and winter hit both segments, the toll hits only one.
WITH daily AS (
  SELECT service_date, service, crz_segment, sum(trips) AS trips FROM gold_daily_segment GROUP BY ALL
), chg AS (
  SELECT service, crz_segment,
         100 * (avg(CASE WHEN service_date BETWEEN DATE'2025-01-05' AND DATE'2025-02-28' THEN trips END)
              / avg(CASE WHEN service_date BETWEEN DATE'2024-11-01' AND DATE'2024-12-31' THEN trips END) - 1) AS pct
  FROM daily GROUP BY ALL
)
SELECT service,
       round(max(CASE WHEN crz_segment = 'touches_zone' THEN pct END), 1)  AS touches_zone_pct,
       round(max(CASE WHEN crz_segment = 'outside' THEN pct END), 1)       AS outside_pct,
       round(max(CASE WHEN crz_segment = 'touches_zone' THEN pct END)
           - max(CASE WHEN crz_segment = 'outside' THEN pct END), 1)       AS gap_pct_points
FROM chg GROUP BY service ORDER BY service;

-- BQ1 sensitivity: drop the holiday fortnight 20 December - 5 January from both windows.
--   before = 2024-11-01 .. 2024-12-19, after = 2025-01-06 .. 2025-02-28. Does the gap keep its sign?
WITH daily AS (
  SELECT service_date, service, crz_segment, sum(trips) AS trips FROM gold_daily_segment GROUP BY ALL
), chg AS (
  SELECT service, crz_segment,
         100 * (avg(CASE WHEN service_date BETWEEN DATE'2025-01-06' AND DATE'2025-02-28' THEN trips END)
              / avg(CASE WHEN service_date BETWEEN DATE'2024-11-01' AND DATE'2024-12-19' THEN trips END) - 1) AS pct
  FROM daily GROUP BY ALL
)
SELECT service,
       round(max(CASE WHEN crz_segment = 'touches_zone' THEN pct END), 1)  AS touches_zone_pct,
       round(max(CASE WHEN crz_segment = 'outside' THEN pct END), 1)       AS outside_pct,
       round(max(CASE WHEN crz_segment = 'touches_zone' THEN pct END)
           - max(CASE WHEN crz_segment = 'outside' THEN pct END), 1)       AS gap_pct_points
FROM chg GROUP BY service ORDER BY service;

-- Sanity check the zone list first (Task 2.3): should be Manhattan, roughly south of 60th Street.
SELECT zone_id, zone, borough, pickups_2025, round(fee_share, 3) AS fee_share
FROM gold_crz_zones WHERE in_crz ORDER BY zone;

-- =============================================================================================
-- BQ2. Who pays the toll, and what happened to driver pay?  (HVFHV only)
--   fee share and average fee: toll era (5 Jan - 28 Feb 2025)
--   driver share = sum(driver_pay) / sum(base passenger fare). The congestion fee is a pass-through
--   and is NOT added to the fare. Denominator: base_passenger_fare (no tolls, taxes, fees, tips).
-- =============================================================================================
SELECT platform,
       sum(trips)                                                    AS trips,
       round(100 * sum(trips_with_fee) / sum(trips), 1)              AS pct_trips_with_fee,
       round(sum(total_cbd_fee) / nullif(sum(trips_with_fee), 0), 2) AS avg_fee_when_charged
FROM gold_fee_summary
WHERE service = 'hvfhv' AND fee_era = 'toll'
GROUP BY platform ORDER BY trips DESC;

-- Before (pre_toll: 1 Nov - 4 Jan) versus after (toll: 5 Jan - 28 Feb), by platform and segment.
SELECT platform, crz_segment, fee_era,
       sum(trips)                                                    AS trips,
       round(sum(total_passenger_fare) / sum(trips), 2)              AS avg_passenger_fare,
       round(sum(total_driver_pay) / sum(trips), 2)                  AS avg_driver_pay,
       round(sum(total_driver_pay) / sum(total_passenger_fare), 3)   AS driver_share
FROM gold_fee_summary
WHERE service = 'hvfhv'
GROUP BY ALL
ORDER BY platform, crz_segment, fee_era DESC;

-- =============================================================================================
-- BQ3. Did service levels change?  wait = pickup - request, HVFHV only.
--   Percentiles come from gold_wait_era (computed over each whole era) — never average daily p90s.
-- =============================================================================================
-- p50 / p90 by platform and segment, all hours together
SELECT platform, crz_segment, fee_era, trips, negative_waits_excluded,
       round(p50_wait_min, 2) AS p50_wait_min, round(p90_wait_min, 2) AS p90_wait_min
FROM gold_wait_era
WHERE hour_scope = 'all_hours'
ORDER BY platform, crz_segment, fee_era DESC;

-- p90 by hour of day, before vs after, touches_zone only (chart: x = hour, one line per era)
SELECT pickup_hour, platform, fee_era, round(p90_wait_min, 2) AS p90_wait_min
FROM gold_wait_era
WHERE hour_scope = 'by_hour' AND crz_segment = 'touches_zone'
ORDER BY platform, pickup_hour, fee_era DESC;

-- How many negative waits were excluded, in total? (say this number in the answer)
SELECT sum(negative_waits_excluded) AS negative_waits_excluded, sum(trips) AS trips,
       round(100 * sum(negative_waits_excluded) / sum(trips), 3) AS pct
FROM gold_wait_era WHERE hour_scope = 'all_hours';

-- =============================================================================================
-- BQ4. How much does weather move demand?  Central Park (USW00094728).
--   Compare WITHIN a month: a snowy January day against a dry January day, not a dry November day.
-- =============================================================================================
SELECT period, service, weather_class,
       count(*)              AS days,
       round(avg(trips))     AS trips_per_day
FROM gold_weather_demand
GROUP BY ALL
ORDER BY period, service, weather_class;

-- Within-month effect: each class relative to that month's dry days (+/- %).
WITH m AS (
  SELECT period, service, weather_class, count(*) AS days, avg(trips) AS tpd
  FROM gold_weather_demand GROUP BY ALL
)
SELECT a.period, a.service, a.weather_class, a.days,
       round(100 * (a.tpd / d.tpd - 1), 1) AS pct_vs_dry_days_same_month
FROM m a JOIN m d ON a.period = d.period AND a.service = d.service AND d.weather_class = 'dry'
WHERE a.weather_class <> 'dry'
ORDER BY a.period, a.service, a.weather_class;
