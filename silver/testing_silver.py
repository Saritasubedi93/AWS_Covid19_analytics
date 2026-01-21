"""
Silver layer transformation for COVID-19 testing data.

Reads the CovidTracking state daily testing CSV from the bronze layer
on S3, normalizes dates and state codes, casts numeric fields, and
writes a partitioned Parquet dataset to the silver layer as
testingstandardized.
"""

from pyspark.sql import SparkSession
from common.io_utils import bronze_path, silver_path, read_csv, write_parquet_partitioned
from common.transform_common import upper_trim, to_date_standard, cast_long, add_ymd
from silver.states_lookup import load_states_lookup


def build_testing_silver(spark: SparkSession) -> None:
    """
    Build the silver testingstandardized dataset:

    - Input:  bronze/covid_tracking/states_daily.csv
              Columns include: date (yyyymmdd), state, positive,
              negative, totalTestResults
    - Lookup: bronze/static/states_abv.csv
    - Output: silver/testingstandardized/ as Parquet partitioned by
              statecode, year, month, day

    Output schema:
    - fulldate       (date)
    - statecode      (string, 2-char)
    - statename      (string)
    - teststotalcum  (bigint)
    - testsposcum    (bigint)
    - testsnegcum    (bigint)
    - year           (int)
    - month          (int)
    - day            (int)
    """
    states = load_states_lookup(spark)

    tests_raw = read_csv(
        spark,
        bronze_path("covid_tracking", "covidtracking_states_daily.csv"),
    )

    tests_std = (
        tests_raw
        # date is int yyyymmdd in this file.
        .withColumn("fulldate", to_date_standard("date", "yyyyMMdd"))
        .withColumn("statecode", upper_trim("state"))
        .join(
            states.select("statecode", "statename"),
            on="statecode",
            how="left",
        )
        .withColumn("teststotalcum", cast_long("totalTestResults"))
        .withColumn("testsposcum", cast_long("positive"))
        .withColumn("testsnegcum", cast_long("negative"))
    )

    tests_std = (
        add_ymd(tests_std, "fulldate")
        .select(
            "fulldate",
            "statecode",
            "statename",
            "teststotalcum",
            "testsposcum",
            "testsnegcum",
            "year",
            "month",
            "day",
        )
        .dropna(subset=["fulldate", "statecode"])
    )

    write_parquet_partitioned(
        tests_std,
        silver_path("testingstandardized"),
        partition_cols=["statecode", "year", "month", "day"],
    )
