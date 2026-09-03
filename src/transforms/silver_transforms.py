from pyspark.sql import DataFrame, functions as F

def clean_trades(bronze_df: DataFrame) -> DataFrame:
    """Remove bad records, fix data types, and create a stable trade_id for safe updates"""
    return (
        bronze_df
        .where(F.col("p").isNotNull() & (F.col("p") > 0) & (F.col("v") > 0))
        .withColumn("event_time", (F.col("t") / 1000).cast("timestamp"))
        .withColumn("trade_date", F.to_date("event_time"))
        .withColumn(
            "trade_id",
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
            "trade_id",
        )
        .dropDuplicates(["trade_id"])
    )