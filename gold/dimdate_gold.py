"""
#dimdate_gold.py
Gold layer builder for the date dimension (dimdate).

Reads all distinct dates from the silver cases and testing datasets
and constructs a calendar dimension with dateid, year/month/day,
day-of-week, and weekend flag. Writes Parquet to the gold layer.
"""

from pyspark.sql import SparkSession, functions as F
from common.io_utils import silver_path, gold_path


def build_dimdate(spark: SparkSession) -> None:
    """
    Build the gold dimdate dataset.

    Input:
      - silver/casesstandardized
      - silver/testingstandardized

    Output:
      - gold/dimdate as Parquet

    Output schema:
      - dateid   (int, yyyymmdd)
      - fulldate (date)
      - year     (int)
      - month    (int)
      - day      (int)
      - dow      (int, 1=Sunday..7=Saturday)
      - isweekend (boolean)
    """
    cases = spark.read.parquet(silver_path("casesstandardized"))
    tests = spark.read.parquet(silver_path("testingstandardized"))

    alldates = cases.select("fulldate").union(tests.select("fulldate")).dropDuplicates()

    dimdate = (
        alldates
        .withColumn("dateid", F.date_format("fulldate", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("fulldate"))
        .withColumn("month", F.month("fulldate"))
        .withColumn("day", F.dayofmonth("fulldate"))
        .withColumn("dow", F.dayofweek("fulldate").cast("int"))
        .withColumn("isweekend", F.col("dow").isin(1, 7))
    )

    dimdate.write.mode("overwrite").parquet(gold_path("dimdate"))
