# Databricks notebook source
# src/ingest/load_reference.py — COPY INTO is idempotent: a re-run loads only files it has not seen
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
CAT, ENV = dbutils.widgets.get("catalog"), dbutils.widgets.get("env")
LH = f"{CAT}.{ENV}_lakehouse"
REF = f"/Volumes/{CAT}/{ENV}_landing/raw/reference"

try:
    landed = [f.name for f in dbutils.fs.ls(REF)]
except Exception:
    landed = []
if not landed:
    dbutils.notebook.exit("no reference files have landed yet")

spark.sql(f"CREATE TABLE IF NOT EXISTS {LH}.ref_taxi_zone")    # schemaless until the first COPY INTO

# COMMAND ----------

display(spark.sql(f"""
    COPY INTO {LH}.ref_taxi_zone
    FROM '{REF}/'
    FILEFORMAT = CSV
    PATTERN = 'taxi_zone_lookup*.csv'
    FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
    COPY_OPTIONS ('mergeSchema' = 'true')"""))                 # num_inserted_rows is 0 on the second run

# COMMAND ----------

# Evidence E5: run this notebook twice. The first run inserts 265 rows, the second 0.
display(spark.sql(f"SELECT count(*) AS zones, count(DISTINCT LocationID) AS distinct_ids FROM {LH}.ref_taxi_zone"))
