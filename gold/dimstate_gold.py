"""
Gold layer builder for the state dimension (dimstate).

Reads distinct statecode/statename pairs from the silver cases dataset
and writes the state dimension to the gold layer.
"""

from pyspark.sql import SparkSession
from common.io_utils import silver_path, gold_path


def build_dimstate(spark: SparkSession) -> None:
    """
    Build the gold dimstate dataset.

    Input:
      - silver/casesstandardized

    Output:
      - gold/dimstate as Parquet

    Output schema:
      - statecode (string, PK)
      - statename (string)
    """
    cases = spark.read.parquet(silver_path("casesstandardized"))

    dimstate = cases.select("statecode", "statename").dropDuplicates()

    dimstate.write.mode("overwrite").parquet(gold_path("dimstate"))
