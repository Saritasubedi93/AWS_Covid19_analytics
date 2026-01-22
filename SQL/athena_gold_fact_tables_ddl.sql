-- factcasesstatedaily external table
CREATE EXTERNAL TABLE IF NOT EXISTS covid_gold_db.fact_cases_state_daily (
  date_id int,
  state_name string,
  cases_cum bigint,
  deaths_cum bigint,
  new_cases int,
  new_deaths int
)
PARTITIONED BY (state_code string)
STORED AS PARQUET
LOCATION 's3://sarita-awsproject-bucket/covid/gold/fact_cases_state_daily/';

MSCK REPAIR TABLE covid_gold_db.fact_cases_state_daily;


-- facttestingstatedaily external table
CREATE EXTERNAL TABLE IF NOT EXISTS covid_gold_db.fact_testing_statedaily (
  dateid          int,
  statecode       string,
  teststotalcum   bigint,
  testsposcum     bigint,
  testsnegcum     bigint,
  newtests        int,
  positivityrate  double
)
PARTITIONED BY (
  statecode string
)
STORED AS PARQUET
LOCATION 's3://sarita-awsproject-bucket/covid/gold/facttestingstatedaily';

MSCK REPAIR TABLE covid_gold_db.fact_testing_state_daily;
