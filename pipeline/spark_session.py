from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

DELTA_PACKAGE = "io.delta:delta-spark_4.1_2.13:4.2.0"


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("trucking-cost-pipeline")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(
        builder, extra_packages=[DELTA_PACKAGE]
    ).getOrCreate()
