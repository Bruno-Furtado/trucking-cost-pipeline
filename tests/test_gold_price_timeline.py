from datetime import date, timedelta

from pyspark.sql.types import DateType, DoubleType, StructField, StructType

from pipeline.gold import _build_price_timeline_logic


WTI_SCHEMA = StructType([
    StructField("date", DateType(), False),
    StructField("price_usd_bbl", DoubleType(), False),
])

DIESEL_SCHEMA = StructType([
    StructField("week_ending", DateType(), False),
    StructField("price_usd_gal", DoubleType(), False),
])


def _wti_14_consecutive_days(spark):
    start = date(2025, 1, 6)
    rows = [(start + timedelta(days=i), 70.0 + i) for i in range(14)]
    return spark.createDataFrame(rows, WTI_SCHEMA)


def _diesel_two_weeks(spark):
    rows = [
        (date(2025, 1, 6), 4.0),
        (date(2025, 1, 13), 4.5),
    ]
    return spark.createDataFrame(rows, DIESEL_SCHEMA)


def test_forward_fill_diesel_between_weeks(spark):
    df_wti = _wti_14_consecutive_days(spark)
    df_diesel = _diesel_two_weeks(spark)

    result = _build_price_timeline_logic(df_wti, df_diesel).orderBy("date").collect()

    assert len(result) == 14
    by_date = {r["date"]: r for r in result}

    for i in range(7):
        d = date(2025, 1, 6) + timedelta(days=i)
        assert by_date[d]["diesel_usd_gal"] == 4.0
        assert by_date[d]["diesel_week_ending"] == date(2025, 1, 6)

    for i in range(7, 14):
        d = date(2025, 1, 6) + timedelta(days=i)
        assert by_date[d]["diesel_usd_gal"] == 4.5
        assert by_date[d]["diesel_week_ending"] == date(2025, 1, 13)


def test_no_nulls_after_forward_fill(spark):
    df_wti = _wti_14_consecutive_days(spark)
    df_diesel = _diesel_two_weeks(spark)

    result = _build_price_timeline_logic(df_wti, df_diesel)

    null_wti = result.filter("wti_usd_bbl IS NULL").count()
    null_diesel = result.filter("diesel_usd_gal IS NULL").count()
    assert null_wti == 0
    assert null_diesel == 0


def test_output_schema(spark):
    df_wti = _wti_14_consecutive_days(spark)
    df_diesel = _diesel_two_weeks(spark)

    result = _build_price_timeline_logic(df_wti, df_diesel)

    assert set(result.columns) == {
        "date",
        "wti_usd_bbl",
        "diesel_usd_gal",
        "diesel_week_ending",
    }
