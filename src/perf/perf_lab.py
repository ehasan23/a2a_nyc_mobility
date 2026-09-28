# Databricks notebook source
# src/perf/perf_lab.py — run on the prd tables after the backfill (Day 4). Record every number in docs/evidence.md.
# Run each measurement TWICE and record the second (the first pays for warm-up).
# After each cell: "See performance" under the cell -> query profile. Record tasks, bytes, spill.
import time

from pyspark.sql import functions as F

SILVER = "workspace.prd_lakehouse.silver_trips"
ONE_PERIOD = spark.table(SILVER).where(F.col("period") == "2025-01")


def timed(label, df):
    t0 = time.time()
    df.write.format("noop").mode("overwrite").save()   # runs the whole plan, stores nothing (fallback: df.count())
    print(f"{label:<40} {time.time() - t0:7.1f}s")

# COMMAND ----------

# 1) Shuffle partitions: one of the few Spark confs you may set on serverless
for setting in ["auto", "8", "4000"]:
    spark.conf.set("spark.sql.shuffle.partitions", setting)
    timed(f"shuffle.partitions={setting}", spark.table(SILVER).groupBy("pu_zone_id", "pickup_date").count())
spark.conf.set("spark.sql.shuffle.partitions", "auto")

# COMMAND ----------

# 2) Input split size: compare the number of scan tasks in the query profile
for size in ["128MB", "16MB"]:
    spark.conf.set("spark.sql.files.maxPartitionBytes", size)
    timed(f"maxPartitionBytes={size}", ONE_PERIOD.agg(F.sum("trip_miles")))
spark.conf.set("spark.sql.files.maxPartitionBytes", "128MB")   # back to the default

# COMMAND ----------

# 3) Join strategy: autoBroadcastJoinThreshold is not settable on serverless, so use hints
dim = spark.table("workspace.prd_lakehouse.dim_zone")
ONE_PERIOD.join(F.broadcast(dim), ONE_PERIOD["pu_zone_id"] == dim["zone_id"]).explain()          # expect BroadcastHashJoin
ONE_PERIOD.join(dim.hint("merge"), ONE_PERIOD["pu_zone_id"] == dim["zone_id"]).explain()         # expect SortMergeJoin
timed("join: broadcast hint", ONE_PERIOD.join(F.broadcast(dim), ONE_PERIOD["pu_zone_id"] == dim["zone_id"]))
timed("join: merge hint", ONE_PERIOD.join(dim.hint("merge"), ONE_PERIOD["pu_zone_id"] == dim["zone_id"]))

# COMMAND ----------

# 4) Skew and spill on purpose: a window over a low-cardinality key, one period only.
#    Open "See performance" -> query profile. Look for one task doing most of the work, spill, DATA_SKEW.
timed("skewed window", ONE_PERIOD.selectExpr(
    "*", "row_number() OVER (PARTITION BY platform_code ORDER BY pickup_ts) AS rn"))

# COMMAND ----------

#    Fix: add a high-cardinality key so the work spreads out. Compare the profiles.
timed("fixed window", ONE_PERIOD.selectExpr(
    "*", "row_number() OVER (PARTITION BY platform_code, pickup_date ORDER BY pickup_ts) AS rn"))

# COMMAND ----------

# 5) Driver memory: one attempt only. Record what happens, then explain the fix (aggregate first).
try:
    rows = ONE_PERIOD.collect()
    print(f"collected {len(rows):,} rows to the driver")
except Exception as exc:
    print("failed as expected:", type(exc).__name__, str(exc)[:300])

# The fix: bring back an answer, not the data.
display(ONE_PERIOD.groupBy("platform_code").agg(F.count("*").alias("trips"), F.avg("trip_miles").alias("avg_miles")))
