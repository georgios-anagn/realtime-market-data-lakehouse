from pyspark.sql import DataFrame, Window, functions as F


def build_minute_bars(silver_trades: DataFrame) -> DataFrame:
    """1-minute OHLCV candlestick bars per symbol, plus VWAP."""
    return (
        silver_trades
        .groupBy("symbol", F.window("event_time", "1 minute"))
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("volume").alias("volume"),
            F.count("*").alias("trade_count"),
            (F.sum(F.col("price") * F.col("volume")) / F.sum("volume")).alias("vwap"),
        )
        .select(
            "symbol",
            F.col("window.start").alias("bar_start"),
            F.col("window.end").alias("bar_end"),
            "open", "high", "low", "close", "volume", "trade_count", "vwap",
        )
    )


def add_moving_averages(minute_bars: DataFrame) -> DataFrame:
    w5 = Window.partitionBy("symbol").orderBy("bar_start").rowsBetween(-4, 0)
    w15 = Window.partitionBy("symbol").orderBy("bar_start").rowsBetween(-14, 0)
    return (
        minute_bars
        .withColumn("sma_5min", F.avg("close").over(w5))
        .withColumn("sma_15min", F.avg("close").over(w15))
    )


def build_daily_summary(minute_bars: DataFrame) -> DataFrame:
    return (
        minute_bars
        .withColumn("trade_date", F.to_date("bar_start"))
        .groupBy("symbol", "trade_date")
        .agg(
            F.first("open").alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.last("close").alias("close"),
            F.sum("volume").alias("volume"),
            F.sum("trade_count").alias("trade_count"),
        )
        .withColumn(
            "pct_change",
            F.round((F.col("close") - F.col("open")) / F.col("open") * 100, 2),
        )
    )