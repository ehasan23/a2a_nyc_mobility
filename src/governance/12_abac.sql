-- src/governance/12_abac.sql — Task 5.3: two ABAC policies cover every tagged Gold object.
-- Tags describe WHAT the data is; the policy decides WHO sees it. A future Gold object that
-- carries the same tags is covered with no new code.
--
-- Checked against the docs (Sept 2026): "Tables, including streaming tables and materialized views,
-- are the only supported securable type for row filter and column mask policies." So policies DO
-- apply to our Gold materialized views. They do not apply to plain views (v_release_status).

-- 1. Governed tags, once. UI: Catalog -> Govern -> Governed tags -> Create.  Or SQL:
CREATE GOVERNED TAG sensitivity VALUES ('financial', 'restricted', 'location');
CREATE GOVERNED TAG access_scope VALUES ('partner');
-- If this fails because governed tags are unavailable: record it in Environment findings and use
-- the fallback at the bottom of this file.

-- 2. Tag every Gold object EXCEPT gold_platform_daily: it already has a manual row filter and masks,
--    and a column cannot carry both a manual rule and an ABAC rule (the query would be blocked).
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_daily_segment ALTER COLUMN total_driver_pay SET TAGS ('sensitivity' = 'financial');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_fee_summary   ALTER COLUMN total_driver_pay SET TAGS ('sensitivity' = 'financial');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_fee_summary   ALTER COLUMN avg_driver_share SET TAGS ('sensitivity' = 'financial');

ALTER MATERIALIZED VIEW workspace.prd_gold.gold_daily_segment ALTER COLUMN platform_code SET TAGS ('access_scope' = 'partner');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_fee_summary   ALTER COLUMN platform_code SET TAGS ('access_scope' = 'partner');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_wait_times    ALTER COLUMN platform_code SET TAGS ('access_scope' = 'partner');
ALTER MATERIALIZED VIEW workspace.prd_gold.gold_wait_era      ALTER COLUMN platform_code SET TAGS ('access_scope' = 'partner');

-- Check the tags landed where you think:
SELECT table_name, column_name, tag_name, tag_value
FROM workspace.information_schema.column_tags
WHERE schema_name = 'prd_gold'
ORDER BY table_name, column_name;

-- 3. One mask policy and one row-filter policy cover every tagged column in the schema.
--    Both functions check ops.entitlements inside, so the policy can target everyone.
CREATE POLICY mask_financial_measures
ON SCHEMA workspace.prd_gold
COMMENT 'Financial measures are masked unless the reader holds the sensitive entitlement'
COLUMN MASK workspace.prd_ops.mask_money
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag_value('sensitivity', 'financial') AS m
ON COLUMN m;

CREATE POLICY partner_rows
ON SCHEMA workspace.prd_gold
COMMENT 'Partner analysts only see rows for the scope they are entitled to'
ROW FILTER workspace.prd_ops.rf_scope
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag_value('access_scope', 'partner') AS s
USING COLUMNS (s);

SHOW POLICIES ON SCHEMA workspace.prd_gold;                           -- evidence: both policies listed

-- 4. Keep your own entitlement at '*' / true whenever the pipeline refreshes.
--    A refresh evaluates policies as the pipeline OWNER; if the owner were filtered while reading a
--    policy-protected source, the Gold object would permanently hold filtered data. (Our Gold objects
--    read only Silver and gold_crz_zones, which carry no tags — but the habit matters.)

-- 5. Prove it: switch to state 2 from 11_entitlement_states.sql, then query the TAGGED tables.
--    Other platforms' rows disappear and driver pay comes back NULL — on every tagged object.
SELECT platform_code, sum(trips) AS trips, sum(total_driver_pay) AS driver_pay
FROM workspace.prd_gold.gold_daily_segment GROUP BY ALL ORDER BY 1;
SELECT platform_code, fee_era, sum(trips) AS trips, max(avg_driver_share) AS max_share
FROM workspace.prd_gold.gold_fee_summary GROUP BY ALL ORDER BY 1, 2;
SELECT platform_code, count(*) AS rows_visible FROM workspace.prd_gold.gold_wait_era GROUP BY ALL;
--    ...then restore state 3.

-- 6. Did the tags survive the next pipeline refresh? Run the pipeline (or wait for a triggered
--    build), then re-run the information_schema query in step 2 and record the answer (Task 5.3).

-- ---------------------------------------------------------------------------------------------
-- FALLBACK if governed tags are not available: declare the same rules inside gold.sql, as
-- gold_platform_daily does — e.g. for gold_fee_summary:
--   CREATE OR REFRESH MATERIALIZED VIEW ${a2.gold}.gold_fee_summary (
--     ... platform_code STRING, ..., total_driver_pay DOUBLE MASK ${a2.ops}.mask_money,
--     avg_driver_share DOUBLE MASK ${a2.ops}.mask_money )
--   WITH ROW FILTER ${a2.ops}.rf_scope ON (platform_code) AS SELECT ...
-- Rules added with ALTER outside the pipeline definition can be reverted by the next refresh,
-- which is why the fallback lives in gold.sql. Then write in the README what ABAC would have
-- saved you: four objects x rules, repeated by hand, and every future object is unprotected
-- until someone remembers.
-- ---------------------------------------------------------------------------------------------

-- Clean-up / re-run helpers
-- DROP POLICY mask_financial_measures ON SCHEMA workspace.prd_gold;
-- DROP POLICY partner_rows ON SCHEMA workspace.prd_gold;
