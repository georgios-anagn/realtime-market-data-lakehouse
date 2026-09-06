from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from src.transforms.gold_transforms import build_minute_bars
from src.transforms.silver_transforms import clean_trades


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("pytest").getOrCreate()


def test_clean_trades_filters_bad_rows(spark):
    df = spark.createDataFrame(
        [
            ("AAPL", 190.5, 10.0, 1710000000000, None),
            ("AAPL", -5.0, 10.0, 1710000000000, None),   # negative price -> dropped
            ("AAPL", 190.5, 0.0, 1710000000000, None),   # zero volume -> dropped
        ],
        "s STRING, p DOUBLE, v DOUBLE, t LONG, c ARRAY<STRING>",
    )
    result = clean_trades(df)
    assert result.count() == 1


def test_minute_bars_has_correct_ohlc(spark):
    df = spark.createDataFrame(
        [
            ("AAPL", 190.0, 10.0, datetime(2026, 8, 10, 14, 30, 5)),
            ("AAPL", 192.0, 5.0, datetime(2026, 8, 10, 14, 30, 20)),
            ("AAPL", 191.0, 8.0, datetime(2026, 8, 10, 14, 30, 40)),
        ],
        "symbol STRING, price DOUBLE, volume DOUBLE, event_time TIMESTAMP",
    )
    bars = build_minute_bars(df).collect()
    assert len(bars) == 1
    row = bars[0]
    assert row["open"] == 190.0
    assert row["high"] == 192.0
    assert row["low"] == 190.0
    assert row["close"] == 191.0