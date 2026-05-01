from datetime import datetime, timezone

from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from pipeline.config import EIA_DIESEL_BRONZE_PATH, WTI_BRONZE_PATH
from pipeline.sources.eia_diesel import fetch_diesel
from pipeline.sources.wti import fetch_wti

# DESIGN DECISION: bronze schema uses snake_case (not yfinance's CamelCase)
# for Python ergonomics. Trade-off: loses 1:1 traceability with source columns.
# Acceptable here because yfinance schema is well-documented and stable.
WTI_BRONZE_SCHEMA = StructType([
    StructField("date", DateType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("adj_close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("dividends", DoubleType(), True),
    StructField("stock_splits", DoubleType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("ingestion_date", DateType(), True),
])


def ingest_wti_to_bronze(spark):
    pandas_df = fetch_wti()
    spark_df = spark.createDataFrame(pandas_df, schema=WTI_BRONZE_SCHEMA)

    # IDEMPOTENCY STRATEGY: partition replace via replaceWhere on ingestion_date.
    # Atomic DELETE+INSERT on the partition matching today's UTC date, so re-runs
    # on the same day produce the same final state instead of duplicating rows.
    today = datetime.now(timezone.utc).date()

    (
        spark_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingestion_date = '{today}'")
        .partitionBy("ingestion_date")
        .save(WTI_BRONZE_PATH)
    )

    total = spark_df.count()
    min_date = spark_df.agg({"date": "min"}).collect()[0][0]
    max_date = spark_df.agg({"date": "max"}).collect()[0][0]

    print(f"Replaced partition ingestion_date={today}: {total} rows written")
    print(f"Min date: {min_date}")
    print(f"Max date: {max_date}")


DIESEL_BRONZE_SCHEMA = StructType([
    StructField("period", DateType(), True),
    StructField("duoarea", StringType(), True),
    StructField("area_name", StringType(), True),
    StructField("product", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("process", StringType(), True),
    StructField("process_name", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("units", StringType(), True),
    StructField("ingestion_timestamp", TimestampType(), True),
    StructField("ingestion_date", DateType(), True),
])


def ingest_diesel_to_bronze(spark):
    pdf = fetch_diesel()
    # API may add new columns over time; align to schema before createDataFrame.
    pdf = pdf[[c.name for c in DIESEL_BRONZE_SCHEMA.fields]]
    spark_df = spark.createDataFrame(pdf, schema=DIESEL_BRONZE_SCHEMA)

    today = datetime.now(timezone.utc).date()

    (
        spark_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingestion_date = '{today}'")
        .partitionBy("ingestion_date")
        .save(EIA_DIESEL_BRONZE_PATH)
    )

    total = spark_df.count()
    min_period = spark_df.agg({"period": "min"}).collect()[0][0]
    max_period = spark_df.agg({"period": "max"}).collect()[0][0]

    print(f"Replaced partition ingestion_date={today}: {total} rows written")
    print(f"Min period: {min_period}")
    print(f"Max period: {max_period}")


if __name__ == "__main__":
    from pipeline.spark_session import get_spark

    spark = get_spark()
    ingest_wti_to_bronze(spark)
    ingest_diesel_to_bronze(spark)
    spark.stop()
