"""
Silver layer transformation for COVID-19 case data.

Reads the NYTimes state-level cases/deaths CSV from the bronze layer
on S3, normalizes dates and state codes, casts numeric fields, and
writes a partitioned Parquet dataset to the silver layer as
casesstandardized.
"""

from pyspark.sql import SparkSession, functions as F
from common.io_utils import bronze_path, silver_path, read_csv, write_parquet_partitioned
from common.transform_common import to_date_standard, cast_long, add_ymd
from silver.states_lookup import load_states_lookup


def build_cases_silver(spark: SparkSession) -> None:
    """
    Build the silver casesstandardized dataset:

    - Input:  bronze/nytimes/us_states.csv
              Columns: date, state, cases, deaths (plus any extras)
    - Lookup: bronze/static/states_abv.csv
    - Output: silver/casesstandardized/ as Parquet partitioned by
              statecode, year, month, day

    Output schema:
    - fulldate   (date)
    - statecode  (string, 2-char)
    - statename  (string)
    - casescum   (bigint)
    - deathscum  (bigint)
    - year       (int)
    - month      (int)
    - day        (int)
    """
    states = load_states_lookup(spark)

    cases_raw = read_csv(
        spark,
        bronze_path("nytimes", "us_states.csv"),
    )

    cases_std = (
        cases_raw
        # date is e.g. 2020-05-01; adjust format if your file differs.
        .withColumn("fulldate", to_date_standard("date"))
        .withColumn("statenameraw", F.initcap(F.col("state")))
        .join(
            states,
            states.statename == F.col("statenameraw"),
            how="left",
        )
        .withColumn("casescum", cast_long("cases"))
        .withColumn("deathscum", cast_long("deaths"))
    )

    cases_std = (
        add_ymd(cases_std, "fulldate")
        .select(
            "fulldate",
            "statecode",
            "statename",
            "casescum",
            "deathscum",
            "year",
            "month",
            "day",
        )
        .dropna(subset=["fulldate", "statecode"])
    )

    write_parquet_partitioned(
        cases_std,
        silver_path("casesstandardized"),
        partition_cols=["statecode", "year", "month", "day"],
    )
