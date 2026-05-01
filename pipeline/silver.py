from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from pipeline.config import (
    EIA_DIESEL_BRONZE_PATH,
    EIA_DIESEL_SILVER_PATH,
    WTI_BRONZE_PATH,
    WTI_SILVER_PATH,
)


def build_wti_silver(spark):
    # SILVER STRATEGY: full refresh from bronze, dedup by business key (date), keep latest ingestion.
    df = spark.read.format("delta").load(WTI_BRONZE_PATH)

    window = Window.partitionBy("date").orderBy(col("ingestion_timestamp").desc())
    deduped = (
        df.withColumn("_rn", row_number().over(window))
          .filter("_rn = 1")
          .drop("_rn")
    )

    silver = (
        deduped
        .select(
            col("date"),
            col("close").alias("price_usd_bbl"),
            col("open").alias("open_usd_bbl"),
            col("high").alias("high_usd_bbl"),
            col("low").alias("low_usd_bbl"),
            col("volume"),
        )
        .filter("price_usd_bbl IS NOT NULL AND price_usd_bbl > 0")
    )

    (
        silver.write
        .format("delta")
        .mode("overwrite")
        .save(WTI_SILVER_PATH)
    )

    total = silver.count()
    min_date = silver.agg({"date": "min"}).collect()[0][0]
    max_date = silver.agg({"date": "max"}).collect()[0][0]

    print(f"Silver WTI: {total} rows written, date range {min_date} to {max_date}.")


def build_diesel_silver(spark):
    # SILVER STRATEGY: full refresh from bronze, dedup by business key (period), keep latest ingestion.
    df = spark.read.format("delta").load(EIA_DIESEL_BRONZE_PATH)

    df = df.filter("duoarea = 'NUS'")

    window = Window.partitionBy("period").orderBy(col("ingestion_timestamp").desc())
    deduped = (
        df.withColumn("_rn", row_number().over(window))
          .filter("_rn = 1")
          .drop("_rn")
    )

    silver = (
        deduped
        .select(
            col("period").alias("week_ending"),
            col("value").alias("price_usd_gal"),
        )
        .filter("price_usd_gal IS NOT NULL AND price_usd_gal > 0")
    )

    (
        silver.write
        .format("delta")
        .mode("overwrite")
        .save(EIA_DIESEL_SILVER_PATH)
    )

    total = silver.count()
    min_week = silver.agg({"week_ending": "min"}).collect()[0][0]
    max_week = silver.agg({"week_ending": "max"}).collect()[0][0]

    print(f"Silver Diesel: {total} rows written, week_ending range {min_week} to {max_week}.")


if __name__ == "__main__":
    from pipeline.spark_session import get_spark

    spark = get_spark()
    build_wti_silver(spark)
    build_diesel_silver(spark)
    spark.stop()
