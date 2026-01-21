"""
Bronze layer validation script.

Reads the raw CSV inputs from the bronze S3 layout to verify that
the files are present, readable, and roughly match expected schema.
This script does not transform or write data; it is only for checks.
"""

from common.spark_session import create_spark
from common.io_utils import bronze_path, read_csv


def main():
    spark = create_spark("covid-bronze-check")

    nytimes_path = bronze_path("nytimes", "us_states.csv")
    covidtracking_path = bronze_path("covid_tracking", "states_daily.csv")
    states_path = bronze_path("static", "states_abv.csv")

    print(f"Reading NYTimes CSV from: {nytimes_path}")
    nytimes_df = read_csv(spark, nytimes_path)
    nytimes_df.printSchema()
    print("NYTimes row count:", nytimes_df.count())

    print(f"Reading CovidTracking CSV from: {covidtracking_path}")
    covidtracking_df = read_csv(spark, covidtracking_path)
    covidtracking_df.printSchema()
    print("CovidTracking row count:", covidtracking_df.count())

    print(f"Reading states lookup CSV from: {states_path}")
    states_df = read_csv(spark, states_path)
    states_df.printSchema()
    print("States row count:", states_df.count())

    spark.stop()


if __name__ == "__main__":
    main()
