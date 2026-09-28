# Databricks notebook source
# 00_smoke_test.py — run once on Day 1 in a serverless notebook; paste the output into README.md
import requests

HOSTS = {
    "TLC trip files (CloudFront)": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
    "NOAA CDO API v2": "https://www.ncei.noaa.gov/cdo-web/api/v2/datasets",
    "NOAA Access Data Service (fallback)": "https://www.ncei.noaa.gov/access/services/data/v1?dataset=daily-summaries&stations=USW00094728&startDate=2025-01-01&endDate=2025-01-02&format=json",
    "PyPI (control: usually allowed)": "https://pypi.org/simple/requests/",
}

print("1) Outbound internet from serverless compute")
for label, url in HOSTS.items():
    try:
        r = requests.get(url, timeout=15, stream=True)
        print(f"   {label:<38} reachable (HTTP {r.status_code})")   # any HTTP status = reachable
    except Exception as exc:
        print(f"   {label:<38} BLOCKED   ({type(exc).__name__})")

print("2) Who am I")
print("   session_user():", spark.sql("SELECT session_user()").first()[0])

print("3) Secret scopes I can see")
try:
    print("  ", [s.name for s in dbutils.secrets.listScopes()])
except Exception as exc:
    print("   secrets not available:", exc)

# COMMAND ----------
# 4) Row filters work on this compute? (creates and drops a throw-away schema)
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.a2_smoke")
spark.sql("CREATE OR REPLACE TABLE workspace.a2_smoke.t AS SELECT * FROM VALUES ('A', 1), ('B', 2) AS t(k, v)")
spark.sql("CREATE OR REPLACE FUNCTION workspace.a2_smoke.only_a(k STRING) RETURNS BOOLEAN RETURN k = 'A'")
spark.sql("ALTER TABLE workspace.a2_smoke.t SET ROW FILTER workspace.a2_smoke.only_a ON (k)")
print("4) Row filter result (expect only A):", spark.table("workspace.a2_smoke.t").collect())
spark.sql("DROP SCHEMA workspace.a2_smoke CASCADE")
