# src/pipeline/bronze.py — raw, as delivered, plus lineage columns
#
# A pipeline source file, NOT a notebook: no "# Databricks notebook source" header.
# Rule for Bronze: do not rename, filter or fix anything. Add lineage, nothing else.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("a2.landing")      # /Volumes/<catalog>/<env>_landing/raw


def autoload(pattern: str, fmt: str, **options):
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", fmt)
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns"))
    for key, value in options.items():
        reader = reader.option(key, value)
    return (reader.load(f"{LANDING}/{pattern}")
            .withColumn("_source_file", F.col("_metadata.file_name"))
            .withColumn("_ingest_ts", F.current_timestamp()))


@dp.table(name="bronze_hvfhv_trips", comment="High-volume FHV trips exactly as delivered")
def bronze_hvfhv_trips():
    return autoload("trips/fhvhv_tripdata_*.parquet", "parquet")


@dp.table(name="bronze_yellow_trips", comment="Yellow taxi trips exactly as delivered")
def bronze_yellow_trips():
    return autoload("trips/yellow_tripdata_*.parquet", "parquet")


@dp.table(name="bronze_weather", comment="NOAA CDO responses, one JSON document per file")
def bronze_weather():
    return autoload("weather/*.json", "json", multiLine="true")
