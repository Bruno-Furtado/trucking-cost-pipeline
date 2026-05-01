import math
from datetime import date, timedelta

from pyspark.sql.types import DateType, DoubleType, StructField, StructType

from pipeline.gold import _build_wti_diesel_lag_logic


# Period > 90 + LAG so apenas um optimum cai dentro da faixa [0, 90].
PERIOD = 120
LAG = 14
N_DAYS = 365 + LAG + 91

TIMELINE_SCHEMA = StructType([
    StructField("date", DateType(), False),
    StructField("wti_usd_bbl", DoubleType(), True),
    StructField("diesel_usd_gal", DoubleType(), True),
    StructField("diesel_week_ending", DateType(), True),
])


def _synthetic_timeline(spark):
    start = date(2024, 1, 1)
    rows = []
    for i in range(N_DAYS):
        d = start + timedelta(days=i)
        wti = 80.0 + 20.0 * math.sin(2 * math.pi * i / PERIOD)
        diesel = 4.0 + 1.0 * math.sin(2 * math.pi * (i - LAG) / PERIOD)
        rows.append((d, wti, diesel, d))
    return spark.createDataFrame(rows, TIMELINE_SCHEMA)


def test_finds_known_lag(spark):
    df = _synthetic_timeline(spark)
    result = _build_wti_diesel_lag_logic(df)

    optimal = result.filter("is_optimal = true").collect()
    assert len(optimal) == 1
    assert optimal[0]["lag_days"] == LAG


def test_returns_91_rows(spark):
    df = _synthetic_timeline(spark)
    result = _build_wti_diesel_lag_logic(df)
    assert result.count() == 91


def test_only_one_optimal(spark):
    df = _synthetic_timeline(spark)
    result = _build_wti_diesel_lag_logic(df)
    assert result.filter("is_optimal = true").count() == 1
