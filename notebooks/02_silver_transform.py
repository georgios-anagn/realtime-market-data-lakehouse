# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean, dedupe, and upsert trades + company profile dimension

# COMMAND ----------
import sys, os
sys.path.append(os.path.abspath("../src"))
from delta.tables import DeltaTable # type: ignore
from pyspark.sql import functions as F

from transforms.silver_transforms import clean_trades # type: ignore

CATALOG = "market_pipeline"
BRONZE_TABLE = f"{CATALOG}.bronze.trades_raw"
SILVER_TABLE = f"{CATALOG}.silver.trades"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/landing/checkpoints/silver_trades"

# COMMAND ----------
if not spark.catalog.tableExists(SILVER_TABLE): # type: ignore
    spark.sql(f""" 
        CREATE TABLE {SILVER_TABLE} (
            symbol STRING, price DOUBLE, volume DOUBLE,
            event_time TIMESTAMP, trade_date DATE, trade_id STRING
        ) USING DELTA PARTITIONED BY (trade_date)
    """) 


def upsert_batch(batch_df, batch_id):
    clean_df = clean_trades(batch_df)
    target = DeltaTable.forName(spark, SILVER_TABLE) # type: ignore
    (
        target.alias("t")
        .merge(clean_df.alias("s"), "t.trade_id = s.trade_id")
        .whenNotMatchedInsertAll()
        .execute()
    )


bronze_stream = spark.readStream.table(BRONZE_TABLE) # type: ignore

query = (
    bronze_stream.writeStream
    .foreachBatch(upsert_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination() # wait for the data load to finish. Otherwise, running next cell can stop the loading.

# COMMAND ----------
# Company profile dimension: SCD Type 1 upsert (overwrite on change, keyed by symbol)
profiles_bronze = spark.table(f"{CATALOG}.bronze.company_profiles_raw") # type: ignore
profiles_clean = profiles_bronze.select(
    F.col("symbol"),
    F.col("name"),
    F.col("finnhubIndustry").alias("industry"),
    F.col("marketCapitalization").alias("market_cap"),
    F.col("currency"),
)

if not spark.catalog.tableExists(f"{CATALOG}.silver.company_profiles"): # type: ignore
    profiles_clean.write.saveAsTable(f"{CATALOG}.silver.company_profiles")
else:
    target = DeltaTable.forName(spark, f"{CATALOG}.silver.company_profiles") # type: ignore
    (
        target.alias("t")
        .merge(profiles_clean.alias("s"), "t.symbol = s.symbol")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )