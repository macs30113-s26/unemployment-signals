import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType


REST_SCHEMA = StructType([
    StructField("canonical", StringType(), nullable=True),
    StructField("group", StringType(), nullable=True),
    StructField("date", StringType(), nullable=True),
    StructField("views", LongType(), nullable=True),
])


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def read_rest(spark, staging_dir):
    return (
        spark.read.option("compression", "gzip")
        .schema(REST_SCHEMA)
        .json(f"{staging_dir}/*.jsonl.gz")
    )


def with_calendar_columns(df):
    date_col = F.to_date("date")
    return (
        df.withColumn("date", date_col)
        .withColumn("week_start", F.date_trunc("week", date_col).cast("date"))
        .withColumn("month", F.date_trunc("month", date_col).cast("date"))
        .withColumn("year", F.year(date_col))
    )


def aggregate(df, grain_col):
    return (
        df.groupBy(grain_col, "canonical", "group", "year")
        .agg(F.sum("views").alias("views"))
        .withColumnRenamed(grain_col, "period")
    )


def write_curated(agg_df, output_dir):
    (
        agg_df.repartition("year")
        .write.mode("overwrite")
        .partitionBy("year")
        .parquet(output_dir)
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--staging", required=True,
                    help="REST staging dir with per-article JSONL.gz files")
    ap.add_argument("--curated", required=True)
    args = ap.parse_args()

    spark = build_spark("wiki-attention-aggregate")
    spark.sparkContext.setLogLevel("WARN")

    df = read_rest(spark, args.staging)
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
