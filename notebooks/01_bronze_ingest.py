# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: incremental raw ingestion with Auto Loader

# COMMAND ----------
from pyspark.sql import functions as F

CATALOG = "market_pipeline"
LANDING_VOLUME_PATH = f"/Volumes/{CATALOG}/landing/trades_raw"
REFERENCE_VOLUME_PATH = f"/Volumes/{CATALOG}/landing/reference"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/landing/checkpoints/bronze_trades"
BRONZE_TABLE = f"{CATALOG}.bronze.trades_raw"

# COMMAND ----------
# Finnhub trade message fields: s=symbol, p=price, v=volume, t=epoch ms, c=conditions
trades_schema = "s STRING, p DOUBLE, v DOUBLE, t LONG, c ARRAY<STRING>"

raw_stream = (
    spark.readStream # type: ignore
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT_PATH + "/schema")
    .schema(trades_schema)
    .load(LANDING_VOLUME_PATH)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

query = (
    raw_stream.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)
query.awaitTermination() # wait for the data load to finish. Otherwise, running next cell can stop the loading.

# COMMAND ----------
# Reference data (company profiles): small dimension, plain batch overwrite is fine
profiles = spark.read.json(REFERENCE_VOLUME_PATH) # type: ignore
profiles.write.mode("overwrite").saveAsTable(f"{CATALOG}.bronze.company_profiles_raw")