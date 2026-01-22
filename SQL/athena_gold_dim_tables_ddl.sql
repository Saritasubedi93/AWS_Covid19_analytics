--dimdate external table
CREATE EXTERNAL TABLE IF NOT EXISTS covid_gold_db.dimdate (
  dateid     int,
  fulldate   date,
  year       int,
  month      int,
  day        int,
  dow        int,
  isweekend  boolean
)
STORED AS PARQUET
LOCATION 's3://sarita-awsproject-bucket/covid/gold/dimdate';

-- dimstate external table
CREATE EXTERNAL TABLE IF NOT EXISTS covid_gold_db.dimstate (
  statecode string,
  statename string
)
STORED AS PARQUET
LOCATION 's3://sarita-awsproject-bucket/covid/gold/dimstate';
