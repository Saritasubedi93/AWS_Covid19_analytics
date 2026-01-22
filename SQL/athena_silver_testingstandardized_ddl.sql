-- External table for Silver testingstandardized
CREATE EXTERNAL TABLE IF NOT EXISTS covid_silver_db.testing_standardized (
  fulldate      date,
  statecode     string,
  statename     string,
  teststotalcum bigint,
  testsposcum   bigint,
  testsnegcum   bigint,
  year          int,
  month         int,
  day           int
)
PARTITIONED BY (
  statecode string,
  year      int,
  month     int,
  day       int
)
STORED AS PARQUET
LOCATION 's3://YOUR_BUCKET/covid/silver/testingstandardized';

-- Load partitions
MSCK REPAIR TABLE covidsilverdb.testingstandardized;
