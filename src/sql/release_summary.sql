-- src/sql/release_summary.sql — SQL task in the release job; the dashboard's status tile reads this table
-- (through the view <env>_gold.v_release_status, created by setup_objects.py).
-- Parameters arrive as named markers: :release_table and :status_table.
CREATE OR REPLACE TABLE IDENTIFIER(:status_table) AS
SELECT max(certified_at)               AS last_certified_at,
       count(*)                        AS certified_releases,
       max_by(periods, certified_at)   AS last_periods
FROM IDENTIFIER(:release_table);
