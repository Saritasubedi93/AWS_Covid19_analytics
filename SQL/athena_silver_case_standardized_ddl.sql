-- External table for Silver cases_standardized
CREATE EXTERNAL TABLE IF NOT EXISTS covid_silver_db.cases_standardized (
  fulldate   date,
  statecode  string,
  statename  string,
  casescum   bigint,
  deathscum  bigint,
  year       int,
  month      int,
  day        int
)
PARTITIONED BY (
  statecode string,
  year      int,
  month     int,
  day       int
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/covid/silver/casesstandardized';

-- Load partitions
MSCK REPAIR TABLE covidsilverdb.casesstandardized;
