# AWS_Covid19_analytics  
End‑to‑end COVID‑19 analytics pipeline on AWS using PySpark and a bronze–silver–gold medallion design. Raw COVID case and testing CSVs on S3 are standardized into partitioned Parquet and modeled as a star schema, then queried via Athena and Redshift for state‑level trends and positivity. [file:1]

---

## Architecture

- **Bronze:** Raw NYTimes or JHU state‑level cases, CovidTracking tests, and state lookup CSVs landed in S3 under `covid/bronze/...`. [file:1]  
- **Silver:** PySpark cleans and standardizes raw data into partitioned Parquet tables (`casesstandardized`, `testingstandardized`) by state and date. [file:1]  
- **Gold:** Star schema on S3 with `dimdate`, `dimstate`, `factcasesstatedaily`, and `facttestingstatedaily`. [file:1]  
- **Serving:** Glue/Athena external tables for validation, then COPY from Gold S3 into Redshift for warehouse‑style analytics. [file:1]

---

## Data sources

- **AWS COVID‑19 Data Lake:** Curated repository hosting NYTimes, JHU, CovidTracking, and related datasets. [file:1]  
- **Cases:** State‑level daily COVID‑19 confirmed cases and deaths from NYTimes or JHU rollups. [file:1]  
- **Testing:** State‑level daily positive, negative, and total test counts from the COVID Tracking Project. [file:1]  
- **Reference:** Static `states_abv.csv` mapping US state names to 2‑letter postal abbreviations. [file:1]

---

## S3 layout

- **Bucket root:** `s3://<YOUR_BUCKET>/covid/` for all layers. [file:1]  

- **Bronze:**  
  - `bronze/nytimes/us_states.csv` – cases and deaths by state and date. [file:1]  
  - `bronze/covidtracking/states_daily.csv` – test metrics by state and date. [file:1]  
  - `bronze/static/states_abv.csv` – state name ↔ abbreviation lookup. [file:1]  

- **Silver:**  
  - `silver/casesstandardized/` – standardized cases dataset partitioned by `statecode/year/month/day`. [file:1]  
  - `silver/testingstandardized/` – standardized testing dataset with the same partition scheme. [file:1]  

- **Gold:**  
  - `gold/dimdate/`, `gold/dimstate/` – dimension tables. [file:1]  
  - `gold/factcasesstatedaily/`, `gold/facttestingstatedaily/` – fact tables partitioned by `statecode`. [file:1]

---

## Silver transformations (PySpark)

- Normalize all date fields to a `DATE` column `fulldate`, then derive `year`, `month`, and `day`. [file:1]  
- Standardize state identifiers by joining the raw data with `states_abv.csv` to get `statecode` and clean `statename`. [file:1]  
- Cast numeric columns (cases, deaths, positive, negative, total tests) to long for safe aggregation. [file:1]  
- Write `casesstandardized` and `testingstandardized` as Parquet, partitioned by `statecode/year/month/day` for efficient queries. [file:1]

---

## Gold modeling (star schema)

- **dimdate:** One row per calendar date with `dateid` (YYYYMMDD), `fulldate`, `year`, `month`, `day`, `dow`, and `isweekend`. [file:1]  
- **dimstate:** De‑duplicate state information into `statecode` and `statename`. [file:1]  
- **factcasesstatedaily:** Join Silver cases with `dimdate` to compute `newcases` and `newdeaths` via window `lag` over cumulative metrics. [file:1]  
- **facttestingstatedaily:** Join Silver testing with `dimdate` to compute `newtests` and `positivityrate` from cumulative totals. [file:1]

---

## Glue, Athena, and Redshift

- **Glue & Athena:** Register Silver and Gold folders as external tables, repair partitions, and run QA checks on schema, row counts, and date ranges. [file:1]  
- **Redshift:** Create a matching `covidgold` star schema and use `COPY ... FORMAT AS PARQUET` from Gold S3 paths for fast BI and ad‑hoc SQL. [file:1]  
- Example queries include top‑N states by new cases on a given date and 7‑day positivity trends per state. [file:1]

---

## How to run

- Configure S3 bucket names, Glue databases, and Redshift connection details in a central config file. [file:1]  
- Run the **Bronze → Silver** PySpark job to build `casesstandardized` and `testingstandardized`. [file:1]  
- Run the **Silver → Gold** PySpark job, refresh Glue/Athena metadata, and then load Redshift using `COPY` to complete the end‑to‑end pipeline. [file:1]
