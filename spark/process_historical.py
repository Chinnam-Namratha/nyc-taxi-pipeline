"""
process_historical.py
─────────────────────
PySpark script to process NYC TLC Yellow Taxi data for all years (2009–2023),
apply the same cleaning logic as the DBT staging + intermediate models, and
produce a daily revenue aggregate written as year/month-partitioned Parquet.

Usage (local / EMR):
    spark-submit spark/process_historical.py \\
        --input  "s3://nyc-tlc/trip data/yellow_tripdata_*.parquet" \\
        --output "s3://your-bucket/output/agg_daily_revenue/" \\
        --zone-lookup "s3://your-bucket/seeds/taxi_zone_lookup.csv"

Deployment notes:
  ── AWS EMR ───────────────────────────────────────────────────────────────────
  spark-submit \\
    --deploy-mode cluster \\
    --master yarn \\
    --conf spark.sql.adaptive.enabled=true \\
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \\
    --conf spark.sql.adaptive.skewJoin.enabled=true \\
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \\
    s3://your-bucket/scripts/process_historical.py \\
    --input  "s3://nyc-tlc/trip data/yellow_tripdata_*.parquet" \\
    --output "s3://your-bucket/output/agg_daily_revenue/"

  ── AWS Glue (PySpark 3.3 / Glue 4.0) ────────────────────────────────────────
  - Create a Glue PySpark job; upload this script to S3.
  - Pass --input and --output via Glue job parameters (getResolvedOptions).
  - Enable job bookmarks: only unprocessed files are read on each run.
  - Use G.2X workers for the sort-merge join during zone enrichment.
  - IAM role must have s3:GetObject on the input bucket.
"""

from __future__ import annotations

import argparse
import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
# Only these columns are required; extra columns in some years are safely ignored.
_REQUIRED_COLS = {
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type",
}

_OPTIONAL_COLS = {
    "extra",
    "mta_tax",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
}

_ZONE_LOOKUP_URL = (
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
)

# ─────────────────────────────────────────────────────────────────────────────
# Spark session factory
# ─────────────────────────────────────────────────────────────────────────────
def get_spark(app_name: str = "NYC-Taxi-Historical") -> SparkSession:
    """
    Build a SparkSession with settings tuned for large-scale Parquet processing.

    Key configs:
      adaptive.enabled                    — AQE dynamically merges post-shuffle
                                            partitions, fixing the "too many small
                                            files" problem without manual tuning.
      adaptive.skewJoin.enabled           — Automatically splits skewed partitions
                                            (e.g. JFK zone has far more rows than
                                            most zones).
      sources.partitionOverwriteMode=dynamic — Only overwrite partitions that are
                                            written in this job run; leaves other
                                            year/month partitions untouched.
      KryoSerializer                      — Faster, more compact than Java
                                            serialisation for shuffle data.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled",                         "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled",      "true")
        .config("spark.sql.adaptive.skewJoin.enabled",                "true")
        .config("spark.serializer",
                "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.sources.partitionOverwriteMode",           "dynamic")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — Read raw Parquet
# ─────────────────────────────────────────────────────────────────────────────
def read_raw(spark: SparkSession, input_path: str) -> DataFrame:
    """
    Read all Parquet files matching input_path (glob or directory).

    Column pruning: we select only the columns we need immediately after read.
    This allows Spark to push the projection into the Parquet reader, avoiding
    reading column chunks we don't touch (critical for a 1.5B-row dataset).

    Schema evolution: different TLC years have slightly different schemas
    (e.g. congestion_surcharge added in 2019).  mergeSchema=true handles this;
    missing columns are filled with NULL.
    """
    logger.info("Reading raw Parquet from: %s", input_path)

    df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(input_path)
    )

    available = set(df.columns)
    select_cols = [
        c for c in (_REQUIRED_COLS | _OPTIONAL_COLS) if c in available
    ]

    missing = _REQUIRED_COLS - available
    if missing:
        raise ValueError(f"Required columns missing from Parquet: {missing}")

    return df.select(select_cols)


# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — Staging / cleaning (mirrors DBT stg_yellow_trips + int_trips_enriched)
# ─────────────────────────────────────────────────────────────────────────────
def apply_staging(df: DataFrame) -> DataFrame:
    """
    Type-cast, rename, compute derived columns, and apply business-rule filters.

    Mirrors the logic in:
      dbt/models/staging/stg_yellow_trips.sql
      dbt/models/intermediate/int_trips_enriched.sql

    Filter rules (identical to the DBT intermediate layer):
      trip_distance            > 0
      fare_amount              > 0
      passenger_count         >= 1
      trip_duration_minutes  BETWEEN 1 AND 180
      pickup_year            BETWEEN 2009 AND 2023
    """
    # ── Cast and rename ───────────────────────────────────────────────────────
    df = (
        df
        .withColumn("pickup_datetime",    F.col("tpep_pickup_datetime").cast(TimestampType()))
        .withColumn("dropoff_datetime",   F.col("tpep_dropoff_datetime").cast(TimestampType()))
        .withColumn("passenger_count",    F.col("passenger_count").cast(IntegerType()))
        .withColumn("pickup_location_id", F.col("PULocationID").cast(IntegerType()))
        .withColumn("dropoff_location_id",F.col("DOLocationID").cast(IntegerType()))
        .withColumn("payment_type",       F.col("payment_type").cast(IntegerType()))
        .withColumn("trip_distance",      F.col("trip_distance").cast(DoubleType()))
        .withColumn("fare_amount",        F.col("fare_amount").cast(DoubleType()))
        .withColumn("tip_amount",         F.col("tip_amount").cast(DoubleType()))
        .withColumn("total_amount",       F.col("total_amount").cast(DoubleType()))
        # ── Derived columns ───────────────────────────────────────────────────
        .withColumn(
            "trip_duration_minutes",
            (
                F.unix_timestamp("dropoff_datetime")
                - F.unix_timestamp("pickup_datetime")
            ) / 60.0
        )
        .withColumn("pickup_date",  F.to_date("pickup_datetime"))
        .withColumn("pickup_year",  F.year("pickup_datetime"))
        .withColumn("pickup_month", F.month("pickup_datetime"))
        # ── Remove raw columns we've renamed ─────────────────────────────────
        .drop("tpep_pickup_datetime", "tpep_dropoff_datetime",
              "PULocationID", "DOLocationID")
    )

    # ── Business-rule filters ─────────────────────────────────────────────────
    df = df.filter(
        (F.col("trip_distance")         >  0)
        & (F.col("fare_amount")         >  0)
        & (F.col("passenger_count")     >= 1)
        & (F.col("trip_duration_minutes").between(1, 180))
        & (F.col("pickup_year").between(2009, 2023))
    )

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Stage 3 — Zone enrichment (broadcast join)
# ─────────────────────────────────────────────────────────────────────────────
def enrich_with_zones(
    spark: SparkSession,
    df: DataFrame,
    zone_path: str,
) -> DataFrame:
    """
    Join trips to the zone lookup CSV for pickup and dropoff locations.

    The zone lookup is only ~265 rows (~10 KB), well below Spark's default
    broadcast threshold of 10 MB, so Spark will automatically broadcast it
    to every executor — avoiding a shuffle on the multi-hundred-million-row
    trips table entirely.
    """
    zones = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(zone_path)
        .select(
            F.col("LocationID").cast(IntegerType()).alias("location_id"),
            F.col("Borough").alias("borough"),
            F.col("Zone").alias("zone_name"),
        )
    )

    # Prepare two aliases for pickup and dropoff joins.
    pu_zones = zones.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("borough").alias("pickup_borough"),
        F.col("zone_name").alias("pickup_zone"),
    )

    do_zones = zones.select(
        F.col("location_id").alias("do_location_id"),
        F.col("borough").alias("dropoff_borough"),
        F.col("zone_name").alias("dropoff_zone"),
    )

    df = (
        df
        .join(pu_zones,
              df.pickup_location_id == F.col("pu_location_id"), "left")
        .drop("pu_location_id")
        .join(do_zones,
              df.dropoff_location_id == F.col("do_location_id"), "left")
        .drop("do_location_id")
    )

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Stage 4 — Daily revenue aggregation (mirrors DBT agg_daily_revenue)
# ─────────────────────────────────────────────────────────────────────────────
def compute_daily_revenue(df: DataFrame) -> DataFrame:
    """
    Compute the daily revenue aggregate equivalent to the DBT agg_daily_revenue model.

    Cache rationale:
      We cache the cleaned DataFrame before aggregation because:
        a) df.count() (logged below) is an action that triggers full evaluation.
        b) The downstream groupBy triggers another scan.
      Without caching, Spark would re-execute the full lineage (read → filter
      → cast → join) twice.  For ~500M rows across 15 years the lineage
      re-evaluation is expensive; caching the cleaned DataFrame (~6–10 GB
      in-memory) saves significant wall-clock time.
      The cache is unpersisted at the end of this function to free executor
      memory before the write stage.
    """
    df.cache()
    row_count = df.count()
    logger.info("Cleaned DataFrame cached: %d rows", row_count)

    agg = (
        df
        .groupBy("pickup_date", "pickup_year", "pickup_month")
        .agg(
            F.count("*")                      .alias("total_trips"),
            F.round(F.sum("fare_amount"),  2) .alias("total_fare"),
            F.round(F.avg("fare_amount"),  2) .alias("avg_fare"),
            F.round(F.sum("tip_amount"),   2) .alias("total_tips"),
            F.round(F.sum("total_amount"), 2) .alias("total_revenue"),
        )
        # Derive tip_rate_pct after aggregation to avoid division in the agg.
        .withColumn(
            "tip_rate_pct",
            F.round(
                F.when(
                    F.col("total_fare") > 0,
                    F.col("total_tips") / F.col("total_fare") * 100.0,
                ).otherwise(F.lit(None).cast(DoubleType())),
                2,
            ),
        )
        .orderBy("pickup_date")
    )

    df.unpersist()
    return agg


# ─────────────────────────────────────────────────────────────────────────────
# Stage 5 — Write partitioned Parquet output
# ─────────────────────────────────────────────────────────────────────────────
def write_output(df: DataFrame, output_path: str) -> None:
    """
    Write the aggregated DataFrame as year/month-partitioned Parquet.

    Repartition rationale:
      Without repartitioning, Spark may write many tiny files per partition
      (one per shuffle task), creating the "small file problem" that hurts
      downstream readers.  Calling repartition("pickup_year", "pickup_month")
      ensures exactly one output file per partition — 12 years × 12 months =
      144 clean Parquet files.

      partitionOverwriteMode=dynamic (set in SparkSession config) means only
      the partitions written in this job are overwritten; prior partitions
      remain untouched, making the job safely re-runnable without data loss.
    """
    logger.info("Writing partitioned Parquet to: %s", output_path)

    (
        df
        .repartition("pickup_year", "pickup_month")
        .write
        .mode("overwrite")
        .partitionBy("pickup_year", "pickup_month")
        .parquet(output_path)
    )

    logger.info("Write complete.")


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(
        description="NYC Taxi historical daily revenue aggregation (2009–2023)"
    )
    parser.add_argument(
        "--input",
        default="c:/Users/cnamratha/Documents/firmable_assessment/data/yellow_tripdata_*.parquet",
        help="Glob path or S3 prefix for raw Parquet files",
    )
    parser.add_argument(
        "--output",
        default="c:/Users/cnamratha/Documents/firmable_assessment/data/agg_daily_revenue/",
        help="Output path for year/month-partitioned Parquet files",
    )
    parser.add_argument(
        "--zone-lookup",
        default="c:/Users/cnamratha/Documents/firmable_assessment/dbt/seeds/taxi_zone_lookup.csv",
        help="Path or URL to the taxi zone lookup CSV",
    )
    parser.add_argument(
        "--skip-zone-enrichment",
        action="store_true",
        help="Skip zone join (useful if zone CSV is unavailable in the cluster)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ── Pipeline ──────────────────────────────────────────────────────────────
    raw_df     = read_raw(spark, args.input)
    clean_df   = apply_staging(raw_df)

    if not args.skip_zone_enrichment:
        clean_df = enrich_with_zones(spark, clean_df, args.zone_lookup)

    agg_df     = compute_daily_revenue(clean_df)
    write_output(agg_df, args.output)

    spark.stop()
    logger.info("Done.")


if __name__ == "__main__":
    main()
