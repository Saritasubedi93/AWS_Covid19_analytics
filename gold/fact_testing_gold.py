"""
Gold layer builder for the daily state-level testing fact table.

Reads the silver testingstandardized dataset and computes daily new
tests and positivity rate per state using window functions. Writes
Parquet to the gold layer as facttestingstatedaily partitioned by
statecode.
"""

from pyspark.sql import SparkSession, functions as F, window as W
from common.io_utils import silver_path, gold_path


def build_fact_testing(spark: SparkSession) -> None:
    """
    Build the gold facttestingstatedaily dataset.

    Input:
      - silver/testingstandardized

    Output:
      - gold/facttestingstatedaily as Parquet partitioned by statecode

    Output schema:
      - dateid        (int, yyyymmdd)
      - statecode     (string)
      - teststotalcum (bigint)
      - testsposcum   (bigint)
      - testsnegcum   (bigint)
      - newtests      (int)
      - positivityrate (double)
    """
    tests = spark.read.parquet(silver_path("testingstandardized"))

    w2 = W.Window.partitionBy("statecode").orderBy("fulldate")

    facttests = (
        tests
        .withColumn("dateid", F.date_format("fulldate", "yyyyMMdd").cast("int"))
        .withColumn(
            "newtests",
            F.greatest(
                F.col("teststotalcum") - F.lag("teststotalcum").over(w2),
                F.lit(0),
            ),
        )
        .withColumn(
            "positivityrate",
            F.when(
                F.col("teststotalcum") > 0,
                F.col("testsposcum") / F.col("teststotalcum").cast("double"),
            )
        )
        .select(
            "dateid",
            "statecode",
            "teststotalcum",
            "testsposcum",
            "testsnegcum",
            "newtests",
            "positivityrate",
        )
    )

    (
        facttests.write
        .mode("overwrite")
        .partitionBy("statecode")
        .parquet(gold_path("facttestingstatedaily"))
    )
