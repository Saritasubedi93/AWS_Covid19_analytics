"""
Shared transformation helpers used across silver and gold layers.

Contains small, reusable functions for normalizing strings, dates,
and numeric types, and for adding common date breakdown columns.
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Column


def upper_trim(col_name: str) -> Column:
    return F.upper(F.trim(F.col(col_name)))


def to_date_standard(col_name: str, fmt: str | None = None) -> Column:
    col = F.col(col_name).cast("string")
    if fmt:
        return F.to_date(col, fmt)
    return F.to_date(col)


def cast_long(col_name: str) -> Column:
    return F.col(col_name).cast("long")


def add_ymd(df: DataFrame, date_col: str = "fulldate") -> DataFrame:
    return (
        df.withColumn("year", F.year(F.col(date_col)))
          .withColumn("month", F.month(F.col(date_col)))
          .withColumn("day", F.dayofmonth(F.col(date_col)))
    )
