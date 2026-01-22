COPY covid_gold.facttestingstatedaily
FROM 's3://sarita-awsproject-bucket/covid/gold/facttestingstatedaily/'
IAM_ROLE 'arn:aws:iam::732073082324:role/redshiftAdmin'
FORMAT AS PARQUET;

COPY covid_gold.factcasesstatedaily
FROM 's3://sarita-awsproject-bucket/covid/gold/factcasesstatedaily/'
IAM_ROLE 'arn:aws:iam::732073082324:role/redshiftAdmin'
FORMAT AS PARQUET;

DROP TABLE IF EXISTS covid_gold.factcasesstatedaily;

CREATE TABLE covid_gold.factcasesstatedaily (
  dateid    int,
  statecode varchar(2),
  casescum  bigint,
  deathscum bigint,
  newcases  bigint,
  newdeaths bigint
)
DISTKEY(statecode)
SORTKEY(dateid, statecode);

DROP TABLE IF EXISTS covid_gold.facttestingstatedaily;

CREATE TABLE covid_gold.facttestingstatedaily (
  dateid         int,
  statecode      varchar(2),
  teststotalcum  bigint,
  testsposcum    bigint,
  testsnegcum    bigint,
  newtests       bigint,
  positivityrate double precision
)
DISTKEY(statecode)
SORTKEY(dateid, statecode);

INSERT INTO covid_gold.factcasesstatedaily (
  dateid,
  statecode,
  casescum,
  deathscum,
  newcases,
  newdeaths
)
SELECT
  dateid,
  statecode,
  casescum,
  deathscum,
  newcases,
  newdeaths
FROM covid_ext.factcasesstatedaily_ext;

INSERT INTO covid_gold.facttestingstatedaily (
  dateid,
  statecode,
  teststotalcum,
  testsposcum,
  testsnegcum,
  newtests,
  positivityrate
)
SELECT
  dateid,
  statecode,
  teststotalcum,
  testsposcum,
  testsnegcum,
  newtests,
  positivityrate
FROM covid_ext.facttestingstatedaily_ext;
