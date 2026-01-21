"""
Silver layer helper for loading the states lookup dimension from bronze.

Reads the static states_abv.csv from the bronze layer on S3 and
produces a standardized lookup with statecode and statename.
"""

from pyspark.sql import SparkSession, DataFrame, functions as F
from common.io_utils import bronze_path, read_csv
from common.transform_common import upper_trim


def load_states_lookup(spark: SparkSession) -> DataFrame:
    """
    Load the state abbreviation/name lookup from the bronze layer and
    return a DataFrame with standardized columns:

    - statecode: 2-character uppercase state code
    - statename: Proper-cased state name
    """
    path = bronze_path("static", "states_abv.csv")

    states = (
        read_csv(spark, path)
        .select(
            upper_trim("Abbreviation").alias("statecode"),
            F.initcap("State").alias("statename"),
        )
    )
    return states
