# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: OHLC bars, moving averages, daily summary, top movers

# COMMAND ----------
import sys, os
sys.path.append(os.path.abspath("../src"))
from pyspark.sql import functions as F

from transforms.gold_transforms import ( 
    build_daily_summary,
    build_minute_bars,
    add_moving_averages,
)

CATALOG = "market_pipeline"
silver_trades = spark.table(f"{CATALOG}.silver.trades") 

# COMMAND ----------
minute_bars = add_moving_averages(build_minute_bars(silver_trades))
minute_bars.write.mode("overwrite").saveAsTable(f"{CATALOG}.gold.minute_bars")

# COMMAND ----------
daily_summary = build_daily_summary(minute_bars)
daily_summary.write.mode("overwrite").saveAsTable(f"{CATALOG}.gold.daily_summary")

# COMMAND ----------
top_movers = (
    daily_summary
    .join(spark.table(f"{CATALOG}.silver.company_profiles"), "symbol", "left") 
    .orderBy(F.abs("pct_change").desc())
)
top_movers.write.mode("overwrite").saveAsTable(f"{CATALOG}.gold.top_movers")