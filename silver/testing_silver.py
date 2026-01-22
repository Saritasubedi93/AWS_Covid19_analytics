"""
Silver layer transformation for COVID-19 testing data.

Reads the CovidTracking state daily testing CSV from the bronze layer
on S3, normalizes dates and state codes, casts numeric fields, and
writes a partitioned Parquet dataset to the silver layer as
testingstandardized.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from common.io_utils import bronze_path, silver_path, write_parquet_partitioned
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

    # Explicit schema: only columns we need
    testing_schema = StructType(
        [
            StructField("date",             StringType(),  True),
            StructField("state",            StringType(),  True),
            StructField("positive",         IntegerType(), True),
            StructField("negative",         IntegerType(), True),
            StructField("totalTestResults", IntegerType(), True),
        ]
    )

    tests_raw = (
        spark.read
             .option("header", True)
             .schema(testing_schema)
             .csv(bronze_path("covid_tracking", "states_daily.csv"))
             #.limit(200)  # TEMP: used subset for faster test runs
    )

    tests_std = (
        tests_raw
        # date is yyyymmdd
        .withColumn("fulldate", to_date_standard("date", "yyyyMMdd"))
        .withColumn("statecode", upper_trim("state"))
        .join(
            states.select("statecode", "statename"),
            on="statecode",
            how="left",
        )
        .withColumn("teststotalcum", cast_long("totalTestResults"))
        .withColumn("testsposcum",   cast_long("positive"))
        .withColumn("testsnegcum",   cast_long("negative"))
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
        .repartition("statecode", "year")
    )

    write_parquet_partitioned(
        tests_std,
        silver_path("testingstandardized"),
        partition_cols=["statecode", "year", "month", "day"],
    )


if __name__ == "__main__":
    from common.spark_session import create_spark

    spark = create_spark("covid-silver-testing")
    try:
        build_testing_silver(spark)
    finally:
        spark.stop()
