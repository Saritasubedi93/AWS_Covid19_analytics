# AWS_Covid19_analytics
End‑to‑end COVID‑19 analytics pipeline on AWS using PySpark and a bronze–silver–gold medallion design. Raw COVID case and testing CSVs on S3 are standardized into partitioned Parquet and modeled as a star schema, then queried via Athena/Redshift for state‑level trends and positivity.
