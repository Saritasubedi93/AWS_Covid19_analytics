"""
Common I/O utilities for reading from and writing to S3.

Provides helper functions to build bronze/silver/gold paths and
standardized read/write wrappers for CSV and Parquet.
"""

from pyspark.sql import DataFrame, SparkSession
from config.settings import PATHS


def bronze_path(*parts: str) -> str:
    return "/".join([PATHS.bronze_root, *parts])


def silver_path(*parts: str) -> str:
    return "/".join([PATHS.silver_root, *parts])


def gold_path(*parts: str) -> str:
    return "/".join([PATHS.gold_root, *parts])


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .csv(path)
    )


def write_parquet_partitioned(
    df: DataFrame,
    path: str,
    partition_cols: list[str],
    mode: str = "overwrite",
) -> None:
    (
        df.write
        .mode(mode)
        .partitionBy(*partition_cols)
        .parquet(path)
    )


def write_parquet(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
) -> None:
    df.write.mode(mode).parquet(path)
