# Databricks notebook source
# src/setup/setup_objects.py — idempotent; the setup job runs it on every target.
# Creates: 4 schemas, the landing Volume and its folders, the ops tables,
# the release-status view in gold, and the two governance functions.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
CAT, ENV = dbutils.widgets.get("catalog"), dbutils.widgets.get("env")
OPS = f"{CAT}.{ENV}_ops"
GOLD = f"{CAT}.{ENV}_gold"

for layer in ("landing", "lakehouse", "gold", "ops"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{ENV}_{layer}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CAT}.{ENV}_landing.raw")
for folder in ["trips", "reference", "weather"]:            # create them up front so file-arrival triggers have a path to watch
    dbutils.fs.mkdirs(f"/Volumes/{CAT}/{ENV}_landing/raw/{folder}")

# COMMAND ----------

# Operational tables
spark.sql(f"""CREATE TABLE IF NOT EXISTS {OPS}.file_manifest (
    dataset STRING, landed_file STRING, unpacked_file STRING, period STRING,
    expected_rows BIGINT, registered_at TIMESTAMP)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {OPS}.reconciliation (
    run_id STRING, period STRING, dataset STRING, expected_rows BIGINT, bronze_rows BIGINT,
    silver_rows BIGINT, quarantined_rows BIGINT, status STRING, checked_at TIMESTAMP)""")
spark.sql(f"CREATE TABLE IF NOT EXISTS {OPS}.release (run_id STRING, periods STRING, certified_at TIMESTAMP, notes STRING)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {OPS}.incidents (run_id STRING, reason STRING, raised_at TIMESTAMP)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {OPS}.entitlements (user_email STRING, scope_value STRING, can_see_sensitive BOOLEAN)")

# The release job's SQL task replaces this table on every certified release. Creating it
# here (empty, same columns) lets the gold view below exist from day one.
spark.sql(f"""CREATE TABLE IF NOT EXISTS {OPS}.release_status (
    last_certified_at TIMESTAMP, certified_releases BIGINT, last_periods STRING)""")

# Analysts are granted the gold schema only. A view in gold that reads the ops table runs
# with the VIEW OWNER's rights, so the dashboard's status tile works without granting ops.
spark.sql(f"""CREATE OR REPLACE VIEW {GOLD}.v_release_status
COMMENT 'Latest certified release, for the dashboard status tile'
AS SELECT * FROM {OPS}.release_status""")

# COMMAND ----------

# Give yourself full access once, so your pipeline, dashboard and queries keep working.
spark.sql(f"""
MERGE INTO {OPS}.entitlements t
USING (SELECT session_user() AS user_email) s
ON t.user_email = s.user_email
WHEN NOT MATCHED THEN INSERT (user_email, scope_value, can_see_sensitive) VALUES (s.user_email, '*', true)""")

# Governance functions used by row filters, column masks and ABAC policies (Day 5).
# Both look the caller up in ops.entitlements, so they need no groups at all — which
# matters on Free Edition, where groups may be workspace-local (see Day 36).
spark.sql(f"""
CREATE OR REPLACE FUNCTION {OPS}.rf_scope(scope STRING)
RETURNS BOOLEAN
COMMENT 'Row filter: caller sees a row when entitled to its scope value, or to *'
RETURN EXISTS (
  SELECT 1 FROM {OPS}.entitlements e
  WHERE e.user_email = session_user() AND (e.scope_value = '*' OR e.scope_value = scope))""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {OPS}.mask_money(v DOUBLE)
RETURNS DOUBLE
COMMENT 'Column mask: financial values only for entitled readers'
RETURN CASE WHEN EXISTS (
    SELECT 1 FROM {OPS}.entitlements e
    WHERE e.user_email = session_user() AND e.can_see_sensitive) THEN v ELSE NULL END""")

display(spark.sql(f"SHOW TABLES IN {OPS}"))
