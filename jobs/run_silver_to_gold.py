"""
Job entrypoint to build gold-layer dimensional and fact tables from silver.

Creates a SparkSession and runs all gold builders to produce the
dimdate, dimstate, factcasesstatedaily, and facttestingstatedaily
Parquet datasets on S3.
"""

from common.spark_session import create_spark
from gold.dimdate_gold import build_dimdate
from gold.dimstate_gold import build_dimstate
from gold.fact_cases_gold import build_fact_cases
from gold.fact_testing_gold import build_fact_testing


def main() -> None:
    spark = create_spark("covid-silver-to-gold")
    try:
        build_dimdate(spark)
        build_dimstate(spark)
        build_fact_cases(spark)
        build_fact_testing(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
