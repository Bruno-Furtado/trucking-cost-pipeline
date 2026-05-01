from pyspark.sql.functions import col, corr, date_sub, lag, last, lit, max as spark_max, row_number
from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType
from pyspark.sql.window import Window

from pipeline.config import (
    CORRELATION_LAG_MAX,
    CORRELATION_LAG_MIN,
    EIA_DIESEL_SILVER_PATH,
    FUEL_SHOCKS_GOLD_PATH,
    PRICE_TIMELINE_GOLD_PATH,
    SHOCK_SUPPRESSION_DAYS,
    SHOCK_TOP_N,
    SHOCK_WINDOW_DAYS,
    WTI_DIESEL_LAG_GOLD_PATH,
    WTI_SILVER_PATH,
)


def _build_price_timeline_logic(df_wti, df_diesel):
    # GOLD STRATEGY: full refresh from silver. Forward fill diesel to daily granularity.
    # Output is consumer-ready: 1 row per WTI trading day, with WTI and most-recent diesel inline.
    wti = df_wti.select("date", "price_usd_bbl")
    diesel = df_diesel.selectExpr(
        "week_ending as date",
        "price_usd_gal",
        "week_ending as diesel_week_ending",
    )

    all_dates = (
        wti.select("date")
        .union(diesel.select("date"))
        .distinct()
    )

    joined = (
        all_dates
        .join(wti, "date", "left")
        .join(diesel, "date", "left")
    )

    window = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    filled = (
        joined
        .withColumn("diesel_usd_gal", last("price_usd_gal", ignorenulls=True).over(window))
        .withColumn("diesel_week_ending_filled", last("diesel_week_ending", ignorenulls=True).over(window))
    )

    final = (
        filled
        .select(
            col("date"),
            col("price_usd_bbl").alias("wti_usd_bbl"),
            col("diesel_usd_gal"),
            col("diesel_week_ending_filled").alias("diesel_week_ending"),
        )
        .filter("wti_usd_bbl IS NOT NULL")
    )

    return final


def build_price_timeline(spark):
    df_wti = spark.read.format("delta").load(WTI_SILVER_PATH)
    df_diesel = spark.read.format("delta").load(EIA_DIESEL_SILVER_PATH)

    final = _build_price_timeline_logic(df_wti, df_diesel)

    (
        final.write
        .format("delta")
        .mode("overwrite")
        .save(PRICE_TIMELINE_GOLD_PATH)
    )

    total = final.count()
    min_date = final.agg({"date": "min"}).collect()[0][0]
    max_date = final.agg({"date": "max"}).collect()[0][0]

    print(f"Gold price_timeline: {total} rows, range {min_date} to {max_date}")


def _build_wti_diesel_lag_logic(df_timeline):
    # GOLD STRATEGY: cross-correlation analysis between WTI and Diesel.
    # For each lag in [CORRELATION_LAG_MIN, CORRELATION_LAG_MAX], compute Pearson correlation
    # between WTI shifted by N days and Diesel current. Optimal lag = max correlation.
    spark = df_timeline.sparkSession
    window = Window.orderBy("date")

    results = []
    for n_days in range(CORRELATION_LAG_MIN, CORRELATION_LAG_MAX + 1):
        lagged = (
            df_timeline.withColumn("wti_lagged", lag("wti_usd_bbl", n_days).over(window))
                       .filter("wti_lagged IS NOT NULL")
        )
        correlation = lagged.agg(corr("wti_lagged", "diesel_usd_gal").alias("c")).collect()[0]["c"]
        results.append((n_days, float(correlation)))

    schema = StructType([
        StructField("lag_days", IntegerType(), False),
        StructField("correlation", DoubleType(), True),
    ])
    result_df = spark.createDataFrame(results, schema=schema)

    max_corr = result_df.agg(spark_max("correlation").alias("max_c")).collect()[0]["max_c"]
    final = result_df.withColumn("is_optimal", col("correlation") == lit(max_corr))

    return final


def build_wti_diesel_lag(spark):
    df = spark.read.format("delta").load(PRICE_TIMELINE_GOLD_PATH)

    final = _build_wti_diesel_lag_logic(df)

    (
        final.write
        .format("delta")
        .mode("overwrite")
        .save(WTI_DIESEL_LAG_GOLD_PATH)
    )

    optimal = final.filter("is_optimal = true").collect()[0]
    print(f"Gold wti_diesel_lag: {final.count()} rows, optimal lag = {optimal['lag_days']} days (corr = {optimal['correlation']:.4f})")


def _build_fuel_shocks_logic(df_timeline):
    # GOLD STRATEGY: detect top-N largest diesel price shocks via rolling 4-week window.
    # Non-maximum suppression (+/- 14 days) ensures each event is represented once.
    window_lag = Window.orderBy("date")
    with_change = (
        df_timeline
        .withColumn("diesel_start_usd_gal", lag("diesel_usd_gal", SHOCK_WINDOW_DAYS).over(window_lag))
        .filter("diesel_start_usd_gal IS NOT NULL")
        .withColumn(
            "pct_change_28d",
            (col("diesel_usd_gal") / col("diesel_start_usd_gal") - 1) * 100
        )
        .filter("pct_change_28d > 0")
    )

    nms_window = Window.orderBy("date").rowsBetween(-SHOCK_SUPPRESSION_DAYS, SHOCK_SUPPRESSION_DAYS)
    candidates = (
        with_change
        .withColumn("local_max", spark_max("pct_change_28d").over(nms_window))
        .filter("pct_change_28d = local_max")
        .drop("local_max")
    )

    # Forward fill semanal do diesel cria dias consecutivos com pct_change_28d identico.
    # Dedup por valor mantendo a primeira data colapsa o cluster em um unico evento.
    dedup_window = Window.partitionBy("pct_change_28d").orderBy("date")
    deduped = (
        candidates
        .withColumn("dedup_rn", row_number().over(dedup_window))
        .filter("dedup_rn = 1")
        .drop("dedup_rn")
    )

    rank_window = Window.orderBy(col("pct_change_28d").desc())
    ranked = (
        deduped
        .withColumn("rank", row_number().over(rank_window).cast(IntegerType()))
        .filter(col("rank") <= SHOCK_TOP_N)
    )

    final = ranked.select(
        col("rank"),
        col("date").alias("peak_date"),
        col("diesel_start_usd_gal"),
        col("diesel_usd_gal").alias("diesel_peak_usd_gal"),
        col("wti_usd_bbl").alias("wti_at_peak_usd_bbl"),
        col("pct_change_28d"),
    )

    final = final.withColumn("period_start_date", date_sub(col("peak_date"), SHOCK_WINDOW_DAYS))

    final = final.select(
        "rank",
        "peak_date",
        "period_start_date",
        "pct_change_28d",
        "diesel_start_usd_gal",
        "diesel_peak_usd_gal",
        "wti_at_peak_usd_bbl",
    )

    return final


def build_fuel_shocks(spark):
    df = spark.read.format("delta").load(PRICE_TIMELINE_GOLD_PATH)

    final = _build_fuel_shocks_logic(df)

    (
        final.write
        .format("delta")
        .mode("overwrite")
        .save(FUEL_SHOCKS_GOLD_PATH)
    )

    print(f"Gold fuel_shocks: {final.count()} rows")
    print("=== Top 5 shocks ===")
    final.orderBy("rank").limit(5).show(truncate=False)


if __name__ == "__main__":
    from pipeline.spark_session import get_spark

    spark = get_spark()
    build_price_timeline(spark)
    build_wti_diesel_lag(spark)
    build_fuel_shocks(spark)
    spark.stop()
