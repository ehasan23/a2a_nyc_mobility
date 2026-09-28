# Databricks notebook source
# src/jobs/quality_check.py — decides the if/else branch through a task value.
# PASS needs all three:
#   1. every reconciliation row of this run is OK (counts balance)
#   2. quarantined rows are at most max_quarantine_pct of Bronze rows
#   3. (our own check) the derived Congestion Relief Zone is plausible: every zone in it is in
#      Manhattan. A zone from another borough means the 90% rule was fooled (for example by a
#      handful of trips), and every "touches_zone" number downstream would be wrong.
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("max_quarantine_pct", "2.0")
CAT, ENV, RUN_ID = dbutils.widgets.get("catalog"), dbutils.widgets.get("env"), dbutils.widgets.get("run_id")
MAX_Q = float(dbutils.widgets.get("max_quarantine_pct"))
OPS, GOLD = f"{CAT}.{ENV}_ops", f"{CAT}.{ENV}_gold"

# COMMAND ----------

rec = spark.table(f"{OPS}.reconciliation").where(F.col("run_id") == RUN_ID)
r = rec.agg(F.count("*").alias("checks"),
            F.sum(F.when(F.col("status") != "OK", 1).otherwise(0)).alias("mismatches"),
            F.sum("bronze_rows").alias("bronze"),
            F.sum("quarantined_rows").alias("quarantined")).first()
quarantine_pct = 100.0 * (r.quarantined or 0) / max(1, r.bronze or 0)

# Own check: the zone list. Empty is fine (no 2025 data yet); a non-Manhattan zone is not.
crz = spark.table(f"{GOLD}.gold_crz_zones").where("in_crz")
crz_zones = crz.count()
bad_zones = [f"{z.zone_id} {z.zone} ({z.borough})" for z in crz.where("borough <> 'Manhattan'").collect()]

reasons = []
if (r.checks or 0) == 0:
    reasons.append("no reconciliation rows for this run")
if (r.mismatches or 0) > 0:
    reasons.append(f"{r.mismatches} reconciliation mismatch(es)")
if quarantine_pct > MAX_Q:
    reasons.append(f"quarantine {quarantine_pct:.3f}% > {MAX_Q}%")
if bad_zones:
    reasons.append(f"non-Manhattan zones in the CRZ list: {bad_zones}")
passed = not reasons

print(f"checks={r.checks} mismatches={r.mismatches} quarantine={quarantine_pct:.3f}% (max {MAX_Q}%) "
      f"crz_zones={crz_zones} non_manhattan={len(bad_zones)} -> {'PASS' if passed else 'FAIL'}")
for reason in reasons:
    print("  reason:", reason)

dbutils.jobs.taskValues.set(key="gate", value="pass" if passed else "fail")
dbutils.jobs.taskValues.set(key="quarantine_pct", value=round(quarantine_pct, 3))
dbutils.jobs.taskValues.set(key="reasons", value="; ".join(reasons) or "none")
