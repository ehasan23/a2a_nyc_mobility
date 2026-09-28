# src/pipeline/silver.py — conform, validate, quarantine, de-duplicate
#
# Flow per service:
#   bronze_<svc>_trips (ST) -> v_<svc>_conformed (temporary view: target schema + derived columns)
#       -> silver_<svc>_clean      (ST, rows that pass every drop rule; warn rules only counted)
#       -> silver_<svc>_quarantine (ST, rows that fail at least one drop rule, with the reasons)
#   clean + quarantine = Bronze, exactly, because every drop rule is wrapped in coalesce(rule, false).
# Then both clean tables -> silver_trips (MV, de-duplicated on _row_key).
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, functions as F

LAKEHOUSE = spark.conf.get("a2.lakehouse")
GOLD = spark.conf.get("a2.gold")

TOLL_START = "2025-01-05"                       # MTA started tolling at 00:00 on Sunday 5 January 2025
STUDY_START, STUDY_END = "2024-11-01", "2025-02-28"   # the analysis window used by Gold (see v_trip_segments)
PLATFORMS = {"HV0002": "Juno", "HV0003": "Uber", "HV0004": "Via", "HV0005": "Lyft"}
PLATFORM_NAME = F.create_map(*[F.lit(x) for pair in PLATFORMS.items() for x in pair])

RAW_DROP_RULES = {
    "dropoff_after_pickup": "dropoff_ts > pickup_ts",
    "duration_1_to_360_min": "trip_time_s BETWEEN 60 AND 21600",
    "distance_0_to_200_mi": "trip_miles BETWEEN 0 AND 200",
}
DROP_RULES = {k: f"coalesce({v}, false)" for k, v in RAW_DROP_RULES.items()}   # a NULL fails the rule
WARN_RULES = {
    "known_zones": "pu_zone_id BETWEEN 1 AND 263 AND do_zone_id BETWEEN 1 AND 263",
    "fare_non_negative": "passenger_fare >= 0",
    "wait_non_negative": "request_ts IS NULL OR pickup_ts >= request_ts",
}
ALL_DROP = " AND ".join(DROP_RULES.values())
KEY_COLS = ["service", "platform_code", "pickup_ts", "dropoff_ts", "pu_zone_id", "do_zone_id",
            "trip_miles", "passenger_fare"]


def col_or_null(df: DataFrame, name: str, dtype: str):
    """A column that only exists in newer files resolves to NULL until it arrives."""
    return F.col(name).cast(dtype) if name in df.columns else F.lit(None).cast(dtype)


def seconds_between(start: str, end: str):
    """Whole seconds from start to end. Works for TIMESTAMP and TIMESTAMP_NTZ alike."""
    return F.expr(f"timestampdiff(SECOND, {start}, {end})")


def conform_hvfhv(df: DataFrame) -> DataFrame:
    return df.select(
        F.lit("hvfhv").alias("service"),
        F.col("hvfhs_license_num").alias("platform_code"),
        PLATFORM_NAME[F.col("hvfhs_license_num")].alias("platform"),
        F.col("request_datetime").alias("request_ts"),
        F.col("pickup_datetime").alias("pickup_ts"),
        F.col("dropoff_datetime").alias("dropoff_ts"),
        F.col("PULocationID").cast("int").alias("pu_zone_id"),
        F.col("DOLocationID").cast("int").alias("do_zone_id"),
        F.col("trip_miles").cast("double").alias("trip_miles"),
        F.col("trip_time").cast("bigint").alias("trip_time_s"),
        F.col("base_passenger_fare").cast("double").alias("passenger_fare"),
        F.col("tips").cast("double").alias("tips"),
        F.col("driver_pay").cast("double").alias("driver_pay"),
        F.coalesce(col_or_null(df, "cbd_congestion_fee", "double"), F.lit(0.0)).alias("cbd_congestion_fee"),
        F.col("_source_file"),
    )


def conform_yellow(df: DataFrame) -> DataFrame:
    """Same target columns, names and types as conform_hvfhv.

    Yellow has no request time and no driver pay, so both are typed NULLs. request_ts takes
    the pickup column's own type (TIMESTAMP or TIMESTAMP_NTZ), so the union in silver_trips
    never has to reconcile two timestamp types.
    """
    ts_type = df.schema["tpep_pickup_datetime"].dataType
    return df.select(
        F.lit("yellow").alias("service"),
        F.lit("YELLOW").alias("platform_code"),
        F.lit("Yellow taxi").alias("platform"),
        F.lit(None).cast(ts_type).alias("request_ts"),
        F.col("tpep_pickup_datetime").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").alias("dropoff_ts"),
        F.col("PULocationID").cast("int").alias("pu_zone_id"),
        F.col("DOLocationID").cast("int").alias("do_zone_id"),
        F.col("trip_distance").cast("double").alias("trip_miles"),
        seconds_between("tpep_pickup_datetime", "tpep_dropoff_datetime").cast("bigint").alias("trip_time_s"),
        F.col("fare_amount").cast("double").alias("passenger_fare"),
        F.col("tip_amount").cast("double").alias("tips"),            # card tips only; cash tips are not recorded
        F.lit(None).cast("double").alias("driver_pay"),
        F.coalesce(col_or_null(df, "cbd_congestion_fee", "double"), F.lit(0.0)).alias("cbd_congestion_fee"),
        F.col("_source_file"),
    )


def add_derived(df: DataFrame) -> DataFrame:
    return (df
            .withColumn("pickup_date", F.to_date("pickup_ts"))
            .withColumn("pickup_hour", F.hour("pickup_ts"))
            .withColumn("period", F.date_format("pickup_ts", "yyyy-MM"))
            .withColumn("fee_era", F.when(F.to_date("pickup_ts") >= F.lit(TOLL_START).cast("date"), "toll")
                                    .otherwise("pre_toll"))
            .withColumn("wait_min", seconds_between("request_ts", "pickup_ts") / 60.0)
            .withColumn("_row_key", F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in KEY_COLS]), 256)))


def quarantine_reasons():
    """The names of the drop rules a row fails, e.g. ["duration_1_to_360_min"]. Explains every quarantined row."""
    return F.array_compact(F.array(*[F.when(~F.expr(rule), F.lit(name)) for name, rule in DROP_RULES.items()]))


# ---- HVFHV: conformed view -> clean streaming table + quarantine streaming table ----
@dp.temporary_view()
def v_hvfhv_conformed():
    return add_derived(conform_hvfhv(spark.readStream.table("bronze_hvfhv_trips")))


@dp.table(name="silver_hvfhv_clean", cluster_by=["pickup_date"], comment="HVFHV trips that pass every drop rule")
@dp.expect_or_fail("has_source_file", "_source_file IS NOT NULL")
@dp.expect_all(WARN_RULES)
@dp.expect_all_or_drop(DROP_RULES)
def silver_hvfhv_clean():
    return spark.readStream.table("v_hvfhv_conformed")


@dp.table(name="silver_hvfhv_quarantine", comment="HVFHV trips that failed at least one drop rule, with the reasons")
def silver_hvfhv_quarantine():
    return (spark.readStream.table("v_hvfhv_conformed")
            .where(f"NOT ({ALL_DROP})")
            .withColumn("quarantine_reasons", quarantine_reasons()))


# ---- Yellow: the same pattern -------------------------------------------------------
@dp.temporary_view()
def v_yellow_conformed():
    return add_derived(conform_yellow(spark.readStream.table("bronze_yellow_trips")))


@dp.table(name="silver_yellow_clean", cluster_by=["pickup_date"], comment="Yellow taxi trips that pass every drop rule")
@dp.expect_or_fail("has_source_file", "_source_file IS NOT NULL")
@dp.expect_all(WARN_RULES)
@dp.expect_all_or_drop(DROP_RULES)
def silver_yellow_clean():
    return spark.readStream.table("v_yellow_conformed")


@dp.table(name="silver_yellow_quarantine", comment="Yellow taxi trips that failed at least one drop rule, with the reasons")
def silver_yellow_quarantine():
    return (spark.readStream.table("v_yellow_conformed")
            .where(f"NOT ({ALL_DROP})")
            .withColumn("quarantine_reasons", quarantine_reasons()))


# ---- de-duplicated, conformed Silver --------------------------------------------------
@dp.materialized_view(name="silver_trips", cluster_by=["pickup_date", "pu_zone_id"],
                      comment="Conformed trips from both services, de-duplicated on _row_key")
def silver_trips():
    hv = spark.read.table("silver_hvfhv_clean")
    ye = spark.read.table("silver_yellow_clean")
    return hv.unionByName(ye).dropDuplicates(["_row_key"])


@dp.materialized_view(name="dim_zone", comment="Taxi zones from the COPY INTO reference table")
def dim_zone():
    return (spark.read.table(f"{LAKEHOUSE}.ref_taxi_zone")
            .select(F.col("LocationID").cast("int").alias("zone_id"), F.col("Borough").alias("borough"),
                    F.col("Zone").alias("zone"), F.col("service_zone")))


@dp.materialized_view(name="silver_weather_daily", comment="One row per station and day; the latest fetch wins")
def silver_weather_daily():
    obs = (spark.read.table("bronze_weather")
           .select("fetched_at", F.explode("results").alias("r"))
           .select("fetched_at",
                   F.to_date(F.substring("r.date", 1, 10)).alias("obs_date"),
                   F.regexp_replace("r.station", "^GHCND:", "").alias("station_id"),
                   F.col("r.datatype").alias("datatype"),
                   F.col("r.value").cast("double").alias("value")))
    # Each scheduled fetch lands a new file: keep the most recently fetched value per station, day and element.
    latest = obs.groupBy("obs_date", "station_id", "datatype").agg(F.max_by("value", "fetched_at").alias("value"))
    # Pipelines do not support .pivot(); conditional aggregation does the same job.
    return (latest.groupBy("obs_date", "station_id")
                  .agg(*[F.max(F.when(F.col("datatype") == t, F.col("value"))).alias(t.lower())
                         for t in ["PRCP", "SNOW", "TMAX", "TMIN"]]))


# ---- shared logic for Gold: every trip in the study window with its zone segment -------
@dp.temporary_view()
def v_trip_segments():
    # Decision: the FILE defines the reconciliation period (ops.reconciliation, per _source_file);
    # the TIMESTAMP defines the analytical period. Trips dated outside Nov 2024 - Feb 2025
    # (a few per file, some years off) are kept in Silver but left out of every Gold object,
    # otherwise stray dates become extra "days" and distort trips-per-day averages.
    crz = spark.read.table(f"{GOLD}.gold_crz_zones").where("in_crz").select("zone_id")
    pu = crz.withColumnRenamed("zone_id", "pu_zone_id").withColumn("pu_in", F.lit(True))
    do = crz.withColumnRenamed("zone_id", "do_zone_id").withColumn("do_in", F.lit(True))
    touches = F.coalesce(F.col("pu_in"), F.lit(False)) | F.coalesce(F.col("do_in"), F.lit(False))
    return (spark.read.table("silver_trips")
            .where(F.col("pickup_date").between(F.lit(STUDY_START).cast("date"), F.lit(STUDY_END).cast("date")))
            .join(F.broadcast(pu), "pu_zone_id", "left")
            .join(F.broadcast(do), "do_zone_id", "left")
            .withColumn("crz_segment", F.when(touches, "touches_zone").otherwise("outside"))
            .drop("pu_in", "do_in"))
