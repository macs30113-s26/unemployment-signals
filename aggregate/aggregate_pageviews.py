import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType


STAGING_SCHEMA = StructType([
    StructField("domain", StringType(), nullable=True),
    StructField("raw_title", StringType(), nullable=True),
    StructField("canonical", StringType(), nullable=True),
    StructField("group", StringType(), nullable=True),
    StructField("views", LongType(), nullable=True),
    StructField("capture_end_utc", StringType(), nullable=True),
])


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.files.maxPartitionBytes", "256m")
        .config("spark.sql.files.openCostInBytes", "16m")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.driver.memory", "16g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def read_staging(spark: SparkSession, staging_dir: str):
    return (
        spark.read.option("compression", "gzip")
        .schema(STAGING_SCHEMA)
        .json(f"{staging_dir}/year=*/month=*/day=*/hour=*.jsonl.gz")
    )


def with_calendar_columns(df):
    # capture_end_utc is the END of the hour per Wikimedia convention,
    # so attribute pageviews to the date of (end - 1h)
    cap_end_ts = F.to_timestamp("capture_end_utc")
    start = cap_end_ts - F.expr("INTERVAL 1 HOUR")
    return (
        df.withColumn("capture_start_utc", start)
        .withColumn("date", F.to_date(start))
        .withColumn("week_start", F.date_trunc("week", start).cast("date"))
        .withColumn("month", F.date_trunc("month", start).cast("date"))
        .withColumn("year", F.year(start))
    )


def aggregate(df, grain_col: str):
    return (
        df.groupBy(grain_col, "canonical", "group", "year")
        .agg(F.sum("views").alias("views"))
        .withColumnRenamed(grain_col, "period")
    )


def write_curated(agg_df, output_dir: str):
    (
        agg_df.repartition("year")
        .write.mode("overwrite")
        .partitionBy("year")
        .parquet(output_dir)
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--staging", required=True)
    ap.add_argument("--curated", required=True)
    args = ap.parse_args()

    spark = build_spark("wiki-attention-aggregate")
    spark.sparkContext.setLogLevel("WARN")

    df = read_staging(spark, args.staging)
    enriched = with_calendar_columns(df).cache()

    n = enriched.count()
    print(f"Staging row count: {n:,}")
    print("Per-group totals:")
    enriched.groupBy("group").agg(F.sum("views").alias("views")).orderBy(
        F.desc("views")
    ).show(truncate=False)

    for grain_col, out_subdir in [
        ("date",       "daily"),
        ("week_start", "weekly"),
        ("month",      "monthly"),
    ]:
        agg = aggregate(enriched, grain_col)
        out_path = f"{args.curated}/{out_subdir}"
        cnt = agg.count()
        print(f"\nWriting {out_path}  rows={cnt:,}")
        write_curated(agg, out_path)

    enriched.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
