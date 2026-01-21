"""
Gold layer builder for the daily state-level cases fact table.

Reads the silver casesstandardized dataset and computes daily new
cases and deaths per state using window functions. Writes Parquet
to the gold layer as factcasesstatedaily partitioned by statecode.
"""

from pyspark.sql import SparkSession, functions as F, window as W
from common.io_utils import silver_path, gold_path


def build_fact_cases(spark: SparkSession) -> None:
    """
    Build the gold factcasesstatedaily dataset.

    Input:
      - silver/casesstandardized

    Output:
      - gold/factcasesstatedaily as Parquet partitioned by statecode

    Output schema:
      - dateid    (int, yyyymmdd)
      - statecode (string)
      - casescum  (bigint)
      - deathscum (bigint)
      - newcases  (int)
      - newdeaths (int)
    """
    cases = spark.read.parquet(silver_path("casesstandardized"))

    w = W.Window.partitionBy("statecode").orderBy("fulldate")

    factcases = (
        cases
        .withColumn("dateid", F.date_format("fulldate", "yyyyMMdd").cast("int"))
        .withColumn(
            "newcases",
            F.greatest(
                F.col("casescum") - F.lag("casescum").over(w),
                F.lit(0),
            ),
        )
        .withColumn(
            "newdeaths",
            F.greatest(
                F.col("deathscum") - F.lag("deathscum").over(w),
                F.lit(0),
            ),
        )
        .select(
            "dateid",
            "statecode",
            "casescum",
            "deathscum",
            "newcases",
            "newdeaths",
        )
    )

    (
        factcases.write
        .mode("overwrite")
        .partitionBy("statecode")
        .parquet(gold_path("factcasesstatedaily"))
    )
