"""
PySpark aggregation job — Layer 2 of the pipeline.

Reads the staging Parquet files written by the Lambda ingest layer,
produces three "curated" datasets in S3:
    s3://$BUCKET/curated/daily/   — (date, canonical, group) → views
    s3://$BUCKET/curated/weekly/  — (week_start, canonical, group) → views
    s3://$BUCKET/curated/monthly/ — (month, canonical, group) → views

Each is written as Parquet partitioned by year, so Athena queries that filter
on a date range only scan the relevant year directories — a real scalable-
computing pattern called partition pruning, worth a paragraph in the README.

Designed to run identically on:
    - single-node Spark via `spark-submit --master local[*]` on one EC2 box, OR
    - multi-node EMR via `spark-submit --master yarn`.
The code uses only RDD/DataFrame primitives that scale to a cluster.

Why this is not a pandas script:
    - Even though our final panel is small (~150k rows), the *staging* layer
      we read from is ~65k partition files. A pandas read loop would be both
      slow (serial S3 GETs) and would gobble memory. Spark parallelizes the
      reads and pushes filtering/aggregation down to per-partition workers.

Run:
    spark-submit aggregate_pageviews.py \
        --bucket my-bucket \
        --staging-prefix staging \
        --curated-prefix curated
"""

import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType


# Mirror the schema written by Lambda (JSON Lines). Declaring it explicitly
# avoids the expensive schema-inference scan over thousands of small files.
# capture_end_utc is written as an ISO string by the JSON encoder; we cast
# to timestamp during enrichment below.
STAGING_SCHEMA = StructType(
    [
        StructField("domain", StringType(), nullable=True),
        StructField("raw_title", StringType(), nullable=True),
        StructField("canonical", StringType(), nullable=True),
        StructField("group", StringType(), nullable=True),
        StructField("views", LongType(), nullable=True),
        StructField("capture_end_utc", StringType(), nullable=True),
    ]
)


def build_spark(app_name: str) -> SparkSession:
    """Build a Spark session tuned for many-small-files S3 reads."""
    return (
        SparkSession.builder.appName(app_name)
        # Coalesce small files so we don't end up with 65k 1-task stages.
        .config("spark.sql.files.maxPartitionBytes", "256m")
        .config("spark.sql.files.openCostInBytes", "16m")
        # Parquet read tuning — push down filters.
        .config("spark.sql.parquet.filterPushdown", "true")
        # When running on a single beefy box (m5.2xlarge = 8 vCPU), local[*]
        # uses all cores. On EMR this is harmless and YARN overrides it.
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )


def read_staging(spark: SparkSession, staging_uri: str):
    """Read the staging gzipped JSON Lines into a DataFrame.

    Spark detects the .gz suffix automatically and decompresses each file as
    its own partition. Files are tiny (~1 KB each after filtering) so we
    rely on spark.sql.files.maxPartitionBytes to coalesce many of them into
    fewer Spark tasks.
    """
    return (
        spark.read.option("compression", "gzip")
        .schema(STAGING_SCHEMA)
        .json(staging_uri + "*/*/*/*.jsonl.gz")
    )


def with_calendar_columns(df):
    """Derive date / week / month columns from the capture timestamp.

    Note: capture_end_utc is a string (ISO format) from JSONL; we cast to
    timestamp here. The Wikimedia convention is that the timestamp in the
    file name is the END of the capture hour, so we attribute pageviews to
    the *date that the hour mostly belongs to* — i.e. capture_start =
    capture_end - 1 hour. This avoids counting the 00:00:00 dump (which
    actually covers 23:00–00:00 of the previous day) against the wrong day.
    """
    cap_end_ts = F.to_timestamp("capture_end_utc")
    start = cap_end_ts - F.expr("INTERVAL 1 HOUR")
    return (
        df.withColumn("capture_end_ts", cap_end_ts)
          .withColumn("capture_start_utc", start)
          .withColumn("date", F.to_date(start))
          .withColumn("week_start", F.date_trunc("week", start).cast("date"))
          .withColumn("month", F.date_trunc("month", start).cast("date"))
          .withColumn("year", F.year(start))
    )


def aggregate(df, grain_col: str):
    """Aggregate to (grain, canonical, group). Sums desktop + mobile.

    Mobile is encoded as domain='en.m' and desktop as 'en'. We sum across
    both to get a single 'total English pageviews' number — splitting them
    would add columns the downstream analysis doesn't use.
    """
    return (
        df.groupBy(grain_col, "canonical", "group", "year")
        .agg(F.sum("views").alias("views"))
        .withColumnRenamed(grain_col, "period")
    )


def write_curated(agg_df, output_uri: str):
    """Write partitioned by year so Athena can prune."""
    (
        agg_df.repartition("year")  # one file per year keeps things tidy
        .write.mode("overwrite")
        .partitionBy("year")
        .parquet(output_uri)
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--staging-prefix", default="staging")
    ap.add_argument("--curated-prefix", default="curated")
    args = ap.parse_args()

    staging_uri = f"s3a://{args.bucket}/{args.staging_prefix}/"
    curated_base = f"s3a://{args.bucket}/{args.curated_prefix}"

    spark = build_spark("pageview-aggregation")
    spark.sparkContext.setLogLevel("WARN")

    staging = read_staging(spark, staging_uri)
    # Cache after the (filter-light) calendar step so each grain reads from
    # memory rather than re-scanning S3.
    enriched = with_calendar_columns(staging).cache()

    print(f"Staging row count: {enriched.count():,}")
    print("Per-group totals:")
    enriched.groupBy("group").agg(F.sum("views").alias("views")).show(truncate=False)

    for grain_col, out_path in [
        ("date",       f"{curated_base}/daily"),
        ("week_start", f"{curated_base}/weekly"),
        ("month",      f"{curated_base}/monthly"),
    ]:
        agg = aggregate(enriched, grain_col)
        print(f"\nWriting {out_path}  rows={agg.count():,}")
        write_curated(agg, out_path)

    enriched.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
