# Databricks notebook source
# src/jobs/reconcile_period.py — runs once per period inside the for-each task.
# For each dataset: expected (Parquet footer) vs Bronze vs Silver clean + quarantined.
# OK means  bronze == expected  AND  clean + quarantined == bronze.
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("period", "")
dbutils.widgets.text("run_id", "manual")
CAT, ENV = dbutils.widgets.get("catalog"), dbutils.widgets.get("env")
PERIOD, RUN_ID = dbutils.widgets.get("period"), dbutils.widgets.get("run_id")
LH, OPS = f"{CAT}.{ENV}_lakehouse", f"{CAT}.{ENV}_ops"

# dataset in the manifest -> (bronze table, silver clean table, quarantine table)
DATASETS = {
    "fhvhv": ("bronze_hvfhv_trips", "silver_hvfhv_clean", "silver_hvfhv_quarantine"),
    "yellow": ("bronze_yellow_trips", "silver_yellow_clean", "silver_yellow_quarantine"),
}

# COMMAND ----------

def rows_from(table: str, files: list) -> int:
    return spark.table(f"{LH}.{table}").where(F.col("_source_file").isin(files)).count()


for dataset, (bronze, clean, quarantine) in DATASETS.items():
    manifest = (spark.table(f"{OPS}.file_manifest")
                .where((F.col("dataset") == dataset) & (F.col("period") == PERIOD)))
    files = [r.f for r in manifest.select(F.coalesce("unpacked_file", "landed_file").alias("f")).collect()]
    if not files:
        continue
    expected = manifest.agg(F.sum("expected_rows")).first()[0] or 0
    b, s, q = rows_from(bronze, files), rows_from(clean, files), rows_from(quarantine, files)
    status = "OK" if (b == expected and s + q == b) else "MISMATCH"
    (spark.createDataFrame(
        [(RUN_ID, PERIOD, dataset, int(expected), b, s, q, status)],
        "run_id string, period string, dataset string, expected_rows bigint, bronze_rows bigint, "
        "silver_rows bigint, quarantined_rows bigint, status string")
     .withColumn("checked_at", F.current_timestamp())
     .write.mode("append").saveAsTable(f"{OPS}.reconciliation"))
    print(f"{dataset} {PERIOD}: expected={expected:,} bronze={b:,} silver={s:,} quarantined={q:,} -> {status}")
