-- Athena DDL — register the curated Parquet output as queryable tables.
--
-- Run these once in the Athena Query Editor after the Spark job has written
-- s3://$BUCKET/curated/{daily,weekly,monthly}/.
--
-- Notes:
--  * "group" is a reserved word, hence the double-quotes everywhere.
--  * year is treated as a Hive partition column so Athena prunes by year,
--    cutting scanned bytes (and therefore cost) for date-bounded queries.
--  * Run MSCK REPAIR TABLE after the first write OR after any subsequent
--    rewrite that adds new year partitions.

CREATE DATABASE IF NOT EXISTS pageviews;

-- ---------- daily ----------
DROP TABLE IF EXISTS pageviews.daily;
CREATE EXTERNAL TABLE pageviews.daily (
    period     DATE,
    canonical  STRING,
    "group"    STRING,
    views      BIGINT
)
PARTITIONED BY (year INT)
STORED AS PARQUET
LOCATION 's3://REPLACE_ME_BUCKET/curated/daily/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
MSCK REPAIR TABLE pageviews.daily;

-- ---------- weekly ----------
DROP TABLE IF EXISTS pageviews.weekly;
CREATE EXTERNAL TABLE pageviews.weekly (
    period     DATE,
    canonical  STRING,
    "group"    STRING,
    views      BIGINT
)
PARTITIONED BY (year INT)
STORED AS PARQUET
LOCATION 's3://REPLACE_ME_BUCKET/curated/weekly/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
MSCK REPAIR TABLE pageviews.weekly;

-- ---------- monthly ----------
DROP TABLE IF EXISTS pageviews.monthly;
CREATE EXTERNAL TABLE pageviews.monthly (
    period     DATE,
    canonical  STRING,
    "group"    STRING,
    views      BIGINT
)
PARTITIONED BY (year INT)
STORED AS PARQUET
LOCATION 's3://REPLACE_ME_BUCKET/curated/monthly/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
MSCK REPAIR TABLE pageviews.monthly;


-- ---------- example queries (for README) ----------

-- Group-level weekly totals over the analysis window.
-- Partition pruning makes this scan only the relevant year partitions.
SELECT period AS week_start,
       "group",
       SUM(views) AS views
FROM pageviews.weekly
WHERE year BETWEEN 2019 AND 2026
GROUP BY period, "group"
ORDER BY period, "group";

-- Top-5 most-viewed articles in the tech displacement group, by year.
SELECT year, canonical, SUM(views) AS yearly_views
FROM pageviews.daily
WHERE "group" = 'tech_displacement'
GROUP BY year, canonical
ORDER BY year, yearly_views DESC;

-- Sanity check: how many Parquet files did Spark write?
-- (Useful in the README as a "before/after" footprint comparison.)
SELECT "$path" AS file_path, COUNT(*) AS rows_in_file
FROM pageviews.weekly
GROUP BY "$path"
ORDER BY file_path;
