"""
Spark session utilities for the COVID-19 analytics project.

Configures Spark to use the S3A filesystem and obtain AWS
credentials from the standard AWS provider chain
(env vars, ~/.aws/credentials, etc.).
"""

from pyspark.sql import SparkSession


def create_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        # Use S3A and the default AWS credentials provider chain
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        .getOrCreate()
    )
