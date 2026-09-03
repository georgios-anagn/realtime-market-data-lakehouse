from pyspark.sql import DataFrame, functions as F

def clean_trades(bronze_df: DataFrame) -> DataFrame:
    """Remove bad records, fix data types, and create a stable trade_id for safe updates"""
    return (
        bronze_df
        .where(F.col("p").isNotNull() & (F.col("p") > 0) & (F.col("v") > 0))
        .withColumn("event_time", (F.col("t") / 1000).cast("timestamp"))
        .withColumn("trade_date", F.to_date("event_time"))
        .withColumn(
            "trade_fingerprint",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("s"),
                    F.col("t").cast("string"),
                    F.col("p").cast("string"),
                    F.col("v").cast("string"),
                ),
                256,
            ),
        )        
        .select(
            F.col("s").alias("symbol"),
            F.col("p").alias("price"),
            F.col("v").alias("volume"),
            "event_time",
            "trade_date",
            "trade_fingerprint",
        )
        # .dropDuplicates(["trade_id"])
    )

"""
    Removed the drop duplicate trade_id method due to removed real trades. The trade_fingerprint does not protect in case Finnhub sends duplicate data (due to no trade ID in the source data). 
    A deterministic SHA-256 fingerprint is retained for analytical comparison, while Auto Loader and Structured Streaming checkpoints provide ingestion/processing state.
"""