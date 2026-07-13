"""
Spark job : enrich ARK ETF holdings history.

INPUT  : s3://bronze/etf_holdings_synthetic/etf_ticker=*/composition_date=*/part.parquet
OUTPUT : s3://silver/etf_holdings_history/etf_ticker=*/composition_year_month=*/part.parquet

ENRICHMENTS :
  - holding_rank : ranked position within the ETF on each date (1 = largest holding)
  - weight_change_pct : day-over-day weight change for each (etf, ticker)
  - is_new_holding : True if this ticker first appeared in this ETF on this date
  - is_exited : True if this ticker disappears the next snapshot day
  - days_since_first_appearance : tenure of the holding in the ETF

WHY SPARK ?
  This dataset (~80k rows) is well within DuckDB's comfort zone — DuckDB
  would actually be faster. The choice to use Spark is pedagogical : we
  want hands-on experience with PySpark window functions, partitions,
  and the Spark UI on a non-trivial dataset.

  See docs/decisions/004-spark-vs-duckdb-for-etf-history.md for full rationale.
"""

import os
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# =====================================================================
# Configuration
# =====================================================================

BRONZE_S3_PATH = "s3a://bronze/etf_holdings_synthetic"
SILVER_S3_PATH = "s3a://silver/etf_holdings_history"

# Schema we expect from the bronze Parquet files
BRONZE_SCHEMA = StructType(
    [
        StructField("etf_ticker", StringType(), nullable=False),
        StructField("company_ticker", StringType(), nullable=False),
        StructField("company_name", StringType(), nullable=True),
        StructField("composition_date", DateType(), nullable=False),
        StructField("weight_pct", DoubleType(), nullable=False),
        StructField("shares_held", DoubleType(), nullable=True),
        StructField("market_value_usd", DoubleType(), nullable=True),
        StructField("issuer", StringType(), nullable=False),
        StructField("fetched_at", TimestampType(), nullable=False),
    ]
)


def build_spark() -> SparkSession:
    """
    Build a SparkSession configured to read from MinIO (S3-compatible).

    Spark needs the hadoop-aws + aws-java-sdk JARs to talk to S3.
    We configure them via 'spark.jars.packages' (Maven coordinates).
    """
    return (
        SparkSession.builder.appName("etf_holdings_history")
        # S3 connector (hadoop-aws is the bridge to S3 via Hadoop FileSystem API)
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        # MinIO connection details
        .config(
            "spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        )
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Sensible defaults for local development
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")  # default 200 is too high locally
        .getOrCreate()
    )


def read_bronze(spark: SparkSession) -> DataFrame:
    """
    Read all bronze Parquet partitions, leveraging Hive partitioning.

    The path glob pattern 'etf_ticker=*/composition_date=*' tells Spark
    to discover partitions automatically and inject them as columns.
    """
    return spark.read.schema(BRONZE_SCHEMA).option("mergeSchema", "false").parquet(BRONZE_S3_PATH)


def enrich_holdings(df: DataFrame) -> DataFrame:
    """
    Add rich window-function-based metrics :
      - holding_rank
      - weight_change_pct
      - is_new_holding
      - is_exited
      - days_since_first_appearance
    """
    # Window 1 : rank holdings within an ETF on each date
    rank_window = Window.partitionBy("etf_ticker", "composition_date").orderBy(
        F.col("weight_pct").desc()
    )

    # Window 2 : day-over-day analysis per (etf, company)
    daily_window = Window.partitionBy("etf_ticker", "company_ticker").orderBy("composition_date")

    # Window 3 : compute first appearance per (etf, company)
    first_appearance_window = Window.partitionBy("etf_ticker", "company_ticker")

    enriched = (
        df
        # Rank within the ETF on the snapshot day
        .withColumn("holding_rank", F.row_number().over(rank_window))
        # Previous snapshot weight (for delta computation)
        .withColumn(
            "previous_weight_pct",
            F.lag("weight_pct").over(daily_window),
        )
        # Previous snapshot date
        .withColumn(
            "previous_composition_date",
            F.lag("composition_date").over(daily_window),
        )
        # Next snapshot date (to detect exits)
        .withColumn(
            "next_composition_date",
            F.lead("composition_date").over(daily_window),
        )
        # Weight change in absolute %
        .withColumn(
            "weight_change_pct",
            F.coalesce(
                F.col("weight_pct") - F.col("previous_weight_pct"),
                F.lit(0.0),
            ),
        )
        # First appearance flag : the row is the first one for this (etf, company)
        .withColumn(
            "is_new_holding",
            F.col("previous_composition_date").isNull(),
        )
        # Exited flag : there's no later snapshot for this (etf, company)
        # NOTE : this is approximate — the holding may just not have been
        # fetched the next day. For accuracy we'd compare to the calendar.
        .withColumn(
            "is_exited",
            F.col("next_composition_date").isNull(),
        )
        # First appearance date for tenure computation
        .withColumn(
            "first_appearance_date",
            F.min("composition_date").over(first_appearance_window),
        )
        .withColumn(
            "days_since_first_appearance",
            F.datediff(F.col("composition_date"), F.col("first_appearance_date")),
        )
        # Add a partition column for output (year-month)
        .withColumn(
            "composition_year_month",
            F.date_format(F.col("composition_date"), "yyyy-MM"),
        )
        # Drop intermediate columns
        .drop(
            "previous_weight_pct",
            "previous_composition_date",
            "next_composition_date",
            "first_appearance_date",
        )
    )

    return enriched


def write_silver(df: DataFrame) -> None:
    """
    Write enriched data to silver layer, partitioned by etf_ticker
    and composition_year_month.
    """
    (
        df.write.mode("overwrite")
        .partitionBy("etf_ticker", "composition_year_month")
        .parquet(SILVER_S3_PATH)
    )


def main() -> None:
    print("=== ETF holdings history enrichment ===")
    print(f"Input  : {BRONZE_S3_PATH}")
    print(f"Output : {SILVER_S3_PATH}")
    print(f"Started at : {datetime.now().isoformat()}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")  # less verbose

    print("\n--- Reading bronze ---")
    bronze_df = read_bronze(spark)
    bronze_count = bronze_df.count()
    print(f"Bronze row count : {bronze_count:,}")
    bronze_df.printSchema()

    print("\n--- Enriching ---")
    enriched_df = enrich_holdings(bronze_df)
    enriched_df.printSchema()

    # Sanity check : new holdings count per ETF
    print("\n--- Sanity check : new holdings per ETF ---")
    enriched_df.groupBy("etf_ticker").agg(
        F.sum(F.col("is_new_holding").cast("int")).alias("nb_new_holdings"),
        F.sum(F.col("is_exited").cast("int")).alias("nb_exited_holdings"),
        F.countDistinct("company_ticker").alias("distinct_companies"),
        F.countDistinct("composition_date").alias("distinct_dates"),
    ).show(truncate=False)

    print("\n--- Writing silver ---")
    write_silver(enriched_df)
    print(f"\n✅ Wrote enriched data to {SILVER_S3_PATH}")
    print(f"Finished at : {datetime.now().isoformat()}")

    spark.stop()


if __name__ == "__main__":
    main()
