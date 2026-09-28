# Databricks notebook source
# src/jobs/certify.py — the "true" branch: record a certified release.
# Appending to ops.release is what fires the release job (table-update trigger).
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("run_id", "manual")
dbutils.widgets.text("periods", "[]")
OPS = f"{dbutils.widgets.get('catalog')}.{dbutils.widgets.get('env')}_ops"

spark.sql("INSERT INTO IDENTIFIER(:t) VALUES (:run_id, :periods, current_timestamp(), 'passed quality gate')",
          args={"t": f"{OPS}.release", "run_id": dbutils.widgets.get("run_id"),
                "periods": dbutils.widgets.get("periods")})
print("certified", dbutils.widgets.get("periods"))
