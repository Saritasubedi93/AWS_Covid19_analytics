"""
Job entrypoint to build silver-layer datasets from bronze CSV inputs.

Creates a SparkSession and runs the case and testing silver
transformations to produce standardized Parquet outputs on S3.
"""

from common.spark_session import create_spark
from silver.cases_silver import build_cases_silver
from silver.testing_silver import build_testing_silver


def main() -> None:
    spark = create_spark("covid-bronze-to-silver")
    try:
        build_cases_silver(spark)
        build_testing_silver(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
