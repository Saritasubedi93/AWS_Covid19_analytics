CREATE SCHEMA IF NOT EXISTS covid_gold;

-- Dimension: date

CREATE TABLE covid_gold.dim_date (
  fulldate   date,
  dateid     int,
  year       int,
  month      int,
  day        int,
  dow        int,
  isweekend  boolean
);



CREATE TABLE covid_gold.dimstate (
  statecode  varchar(2),
  statename  varchar(64)
);

DROP TABLE IF EXISTS covid_gold.factcasesstatedaily;

CREATE TABLE covid_gold.factcasesstatedaily (
  dateid    int,
  casescum  bigint,
  deathscum bigint,
  newcases  bigint,
  newdeaths bigint,
  statecode varchar(2)
)
DISTKEY(statecode)
SORTKEY(dateid, statecode);


CREATE EXTERNAL SCHEMA covid_ext
FROM DATA CATALOG
DATABASE 'covid_gold_db'   -- <-- your Glue/Athena database name
IAM_ROLE 'arn:aws:iam::732073082324:role/redshiftAdmin'
CREATE EXTERNAL DATABASE IF NOT EXISTS;


-- External factcasesstatedaily (reads the partitioned Parquet directly)
CREATE EXTERNAL TABLE covid_ext.factcasesstatedaily_ext (
  dateid    int,
  casescum  bigint,
  deathscum bigint,
  newcases  bigint,
  newdeaths bigint
)
PARTITIONED BY (statecode varchar(2))
STORED AS PARQUET
LOCATION 's3://sarita-awsproject-bucket/covid/gold/factcasesstatedaily/';



CREATE EXTERNAL TABLE covid_ext.facttestingstatedaily_ext (
  dateid         int,
  teststotalcum  bigint,
  testsposcum    bigint,
  testsnegcum    bigint,
  newtests       bigint,
  positivityrate double precision
)
PARTITIONED BY (statecode varchar(2))
STORED AS PARQUET
LOCATION 's3://sarita-awsproject-bucket/covid/gold/facttestingstatedaily/';




