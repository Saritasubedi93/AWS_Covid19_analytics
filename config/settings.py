"""
Configuration module for global project settings such as S3 bucket
and logical prefixes for bronze, silver, and gold layers.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Paths:
    bucket: str = "sarita-awsproject-bucket"
    region: str = "us-east-2"

    bronze_prefix: str = "covid/bronze"
    silver_prefix: str = "covid/silver"
    gold_prefix: str = "covid/gold"

    @property
    def bronze_root(self) -> str:
        return f"s3a://{self.bucket}/{self.bronze_prefix}"

    @property
    def silver_root(self) -> str:
        return f"s3a://{self.bucket}/{self.silver_prefix}"

    @property
    def gold_root(self) -> str:
        return f"s3a://{self.bucket}/{self.gold_prefix}"


PATHS = Paths()
