-- src/governance/10_access.sql — Task 5.1: grants on the gold schema only, REVOKE, and the managed-table lifecycle.
-- Run in the SQL editor on the Serverless Starter Warehouse. Screenshot every SHOW GRANTS (evidence E15).
--
-- Who is the reader? Your invited teammate if the smoke test allowed invites
-- (e.g. `name@bjitgroup.com`), otherwise the built-in group `account users`.

-- The minimum set to query one table: USE CATALOG + USE SCHEMA + SELECT.
GRANT USE CATALOG ON CATALOG workspace TO `account users`;
GRANT USE SCHEMA  ON SCHEMA  workspace.prd_gold TO `account users`;
GRANT SELECT      ON SCHEMA  workspace.prd_gold TO `account users`;   -- inherited by current AND future Gold objects

SHOW GRANTS ON SCHEMA workspace.prd_gold;                             -- screenshot 1: SELECT is listed

REVOKE SELECT ON SCHEMA workspace.prd_gold FROM `account users`;
SHOW GRANTS ON SCHEMA workspace.prd_gold;                             -- screenshot 2: the SELECT row is gone

GRANT SELECT ON SCHEMA workspace.prd_gold TO `account users`;         -- restore it for the demo

-- Nothing on ops or lakehouse: analysts never see raw, quarantine or entitlement data.
SHOW GRANTS ON SCHEMA workspace.prd_ops;

-- ---------------------------------------------------------------------------------------------
-- Managed-table lifecycle: DROP deletes the data files (after the retention window); UNDROP brings
-- the table back within 7 days. An EXTERNAL table's DROP removes only metadata — the files stay in
-- your storage, and there is nothing to UNDROP. (Free Edition has no external locations, so this
-- half is theory: know it for the exam.)
-- ---------------------------------------------------------------------------------------------
CREATE TABLE workspace.prd_ops.scratch_undrop AS SELECT * FROM workspace.prd_gold.gold_crz_zones;
DESCRIBE DETAIL workspace.prd_ops.scratch_undrop;                     -- note: Type = MANAGED
DROP TABLE workspace.prd_ops.scratch_undrop;
SHOW TABLES DROPPED IN workspace.prd_ops;                             -- screenshot: it is listed, with a table id
UNDROP TABLE workspace.prd_ops.scratch_undrop;
SELECT count(*) AS rows_back FROM workspace.prd_ops.scratch_undrop;   -- same count as before the DROP
DROP TABLE workspace.prd_ops.scratch_undrop;                          -- tidy up
