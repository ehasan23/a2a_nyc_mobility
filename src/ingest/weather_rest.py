# Databricks notebook source
# src/ingest/weather_rest.py — NOAA Climate Data Online (CDO) API v2 -> landing Volume as raw JSON
# If the smoke test says www.ncei.noaa.gov is BLOCKED from serverless, run
# tools/weather_fetch_local.py on your laptop instead. It writes the same JSON shape.
import datetime as dt
import json
import time

import requests

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("start_date", "2024-11-01")
dbutils.widgets.text("end_date", "2025-02-28")
dbutils.widgets.text("stations", "GHCND:USW00094728,GHCND:USW00094789,GHCND:USW00014732")   # Central Park, JFK, LaGuardia
CAT, ENV = dbutils.widgets.get("catalog"), dbutils.widgets.get("env")
OUT = f"/Volumes/{CAT}/{ENV}_landing/raw/weather"
BASE = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"

TOKEN = dbutils.secrets.get(scope="a2", key="noaa_token")   # never hard-code credentials
print("token:", TOKEN)   # prints [REDACTED]: screenshot once for the evidence pack (E6), then delete this line

# COMMAND ----------

def year_windows(start: str, end: str):
    """CDO allows at most one year of daily data per request."""
    s, e = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    while s <= e:
        w_end = min(e, dt.date(s.year, 12, 31))
        yield s, w_end
        s = w_end + dt.timedelta(days=1)


def get_page(params: dict, attempts: int = 5) -> dict:
    for attempt in range(1, attempts + 1):
        r = requests.get(BASE, headers={"token": TOKEN}, params=params, timeout=60)
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        return r.json() or {}                  # CDO answers {} when there is no data
    raise RuntimeError(f"CDO kept failing for {params}")


dbutils.fs.mkdirs(OUT)
stations = [s.strip() for s in dbutils.widgets.get("stations").split(",") if s.strip()]
if not stations:                                        # variant B keeps its station mapping in a table
    stations = [r.station_id for r in spark.table(f"{CAT}.{ENV}_lakehouse.ref_airport_station").collect()]
stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")   # every run lands new files

# COMMAND ----------

for station in stations:
    for s, e in year_windows(dbutils.widgets.get("start_date"), dbutils.widgets.get("end_date")):
        rows, offset = [], 1
        while True:
            body = get_page({"datasetid": "GHCND", "stationid": station,
                             "datatypeid": ["PRCP", "SNOW", "TMAX", "TMIN"],
                             "startdate": s.isoformat(), "enddate": e.isoformat(),
                             "units": "metric", "limit": 1000, "offset": offset})
            rows += body.get("results", [])
            total = body.get("metadata", {}).get("resultset", {}).get("count", 0)
            offset += 1000
            time.sleep(0.25)                   # stay under 5 requests per second
            if offset > total:
                break
        name = f"{station.replace(':', '_')}_{s:%Y%m%d}_{e:%Y%m%d}_{stamp}.json"
        doc = {"station": station, "start": s.isoformat(), "end": e.isoformat(),
               "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(), "results": rows}
        dbutils.fs.put(f"{OUT}/{name}", json.dumps(doc), overwrite=False)
        print(f"{name}: {len(rows)} observations")
