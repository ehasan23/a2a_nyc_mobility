# Databricks notebook source
# src/jobs/prepare.py — find newly landed files, register their expected row counts.
# Sets two task values for the rest of the DAG:
#   periods — the list the for-each task iterates over, e.g. ["2024-11", "2024-12"]
#   n_new   — how many periods need work; the has_new condition task routes on it
import re

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
CAT, ENV = dbutils.widgets.get("catalog"), dbutils.widgets.get("env")
LANDING = f"/Volumes/{CAT}/{ENV}_landing/raw/trips"
MANIFEST = f"{CAT}.{ENV}_ops.file_manifest"
RECON = f"{CAT}.{ENV}_ops.reconciliation"
PATTERN = re.compile(r"^(fhvhv|yellow)_tripdata_(\d{4}-\d{2})\.parquet$")

known = {r.landed_file for r in spark.table(MANIFEST).select("landed_file").collect()}
periods = set()

# COMMAND ----------

# 1) Register every file that has landed but is not in the manifest yet.
for f in sorted(dbutils.fs.ls(LANDING), key=lambda f: f.name):
    m = PATTERN.match(f.name)
    if not m or f.name in known:
        continue
    dataset, period = m.group(1), m.group(2)
    expected = spark.read.parquet(f.path).count()          # answered from the Parquet footers
    spark.sql("INSERT INTO IDENTIFIER(:t) VALUES (:ds, :f, NULL, :p, :n, current_timestamp())",
              args={"t": MANIFEST, "ds": dataset, "f": f.name, "p": period, "n": expected})
    periods.add(period)
    print(f"registered {f.name}: {expected:,} rows")

# COMMAND ----------

# 2) Self-healing (added to the starter kit): also pick up periods that were registered by an
#    earlier run but never reconciled OK — for example when that run failed in `build` and
#    someone pressed "Run now" instead of "Repair run". Without this, a registered file is
#    never "new" again, so it would never be reconciled or certified.
unfinished = (spark.table(MANIFEST).select("dataset", "period").distinct()
              .join(spark.table(RECON).where("status = 'OK'").select("dataset", "period").distinct(),
                    ["dataset", "period"], "left_anti")
              .select("period").distinct())
carried = {r.period for r in unfinished.collect()} - periods
if carried:
    print(f"carrying over periods registered earlier but not yet reconciled OK: {sorted(carried)}")
periods |= carried

# COMMAND ----------

dbutils.jobs.taskValues.set(key="periods", value=sorted(periods))
dbutils.jobs.taskValues.set(key="n_new", value=len(periods))
print(f"periods={sorted(periods)} n_new={len(periods)}")
