#!/usr/bin/env python3
"""Laptop fallback for Task 1.4, when serverless compute cannot reach www.ncei.noaa.gov.

Same logic as src/ingest/weather_rest.py, run from your laptop instead:
  - reads the NOAA token from an environment variable (never from code or Git),
  - calls the CDO API v2 in one-year windows, 1,000 results a page, under 5 requests a second,
  - writes one JSON document per station and window, in exactly the shape the notebook writes,
  - uploads it to /Volumes/<catalog>/<env>_landing/raw/weather/ with the Databricks SDK.
The pipeline cannot tell the difference: it still ingests JSON files from the Volume.

If the CDO API itself is down, --source ads uses NOAA's Access Data Service (no token) and
converts its rows into the CDO shape, so Silver does not change either.

    pip install requests databricks-sdk
    export NOAA_TOKEN=...            # Windows PowerShell:  $env:NOAA_TOKEN="..."
    python tools/weather_fetch_local.py --env dev
    python tools/weather_fetch_local.py --env prd
    python tools/weather_fetch_local.py --env dev --source ads   # token-free fallback
"""
import argparse
import datetime as dt
import io
import json
import os
import sys
import time

import requests
from databricks.sdk import WorkspaceClient

CDO = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"
ADS = "https://www.ncei.noaa.gov/access/services/data/v1"
STATIONS = ["GHCND:USW00094728", "GHCND:USW00094789", "GHCND:USW00014732"]   # Central Park, JFK, LaGuardia
ELEMENTS = ["PRCP", "SNOW", "TMAX", "TMIN"]


def year_windows(start: str, end: str):
    s, e = dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    while s <= e:
        w_end = min(e, dt.date(s.year, 12, 31))
        yield s, w_end
        s = w_end + dt.timedelta(days=1)


def cdo_rows(token: str, station: str, s: dt.date, e: dt.date) -> list:
    rows, offset = [], 1
    while True:
        for attempt in range(1, 6):
            r = requests.get(CDO, headers={"token": token}, timeout=60, params={
                "datasetid": "GHCND", "stationid": station, "datatypeid": ELEMENTS,
                "startdate": s.isoformat(), "enddate": e.isoformat(),
                "units": "metric", "limit": 1000, "offset": offset})
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            break
        else:
            raise RuntimeError(f"CDO kept failing for {station} {s}..{e}")
        body = r.json() or {}
        rows += body.get("results", [])
        total = body.get("metadata", {}).get("resultset", {}).get("count", 0)
        offset += 1000
        time.sleep(0.25)
        if offset > total:
            return rows


def ads_rows(station: str, s: dt.date, e: dt.date) -> list:
    """Access Data Service -> the CDO 'results' shape: one row per station, day and element."""
    r = requests.get(ADS, timeout=120, params={
        "dataset": "daily-summaries", "stations": station.split(":")[-1],
        "startDate": s.isoformat(), "endDate": e.isoformat(),
        "dataTypes": ",".join(ELEMENTS), "units": "metric", "format": "json"})
    r.raise_for_status()
    out = []
    for rec in r.json():
        for el in ELEMENTS:
            v = rec.get(el)
            if v not in (None, ""):
                out.append({"date": f"{rec['DATE']}T00:00:00", "datatype": el, "station": station,
                            "attributes": "", "value": float(v)})
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", default="DEFAULT")
    p.add_argument("--catalog", default="workspace")
    p.add_argument("--env", default="dev")
    p.add_argument("--start", default="2024-11-01")
    p.add_argument("--end", default="2025-02-28")
    p.add_argument("--source", choices=["cdo", "ads"], default="cdo")
    args = p.parse_args()

    token = os.environ.get("NOAA_TOKEN", "")
    if args.source == "cdo" and not token:
        sys.exit("set NOAA_TOKEN first (or use --source ads)")

    w = WorkspaceClient(profile=args.profile)
    root = f"/Volumes/{args.catalog}/{args.env}_landing/raw/weather"
    w.files.create_directory(root)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")   # every run lands new files

    for station in STATIONS:
        for s, e in year_windows(args.start, args.end):
            rows = cdo_rows(token, station, s, e) if args.source == "cdo" else ads_rows(station, s, e)
            name = f"{station.replace(':', '_')}_{s:%Y%m%d}_{e:%Y%m%d}_{stamp}.json"
            # Exactly the notebook's keys: an extra key would be a NEW COLUMN to Auto Loader and
            # stop the Bronze weather stream once with UnknownFieldException.
            doc = {"station": station, "start": s.isoformat(), "end": e.isoformat(),
                   "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(), "results": rows}
            w.files.upload(f"{root}/{name}", io.BytesIO(json.dumps(doc).encode()), overwrite=False)
            print(f"{name}: {len(rows)} observations -> {root}")


if __name__ == "__main__":
    main()
