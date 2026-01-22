-- Quick QA checks on Silver layer

#-- Date range and total rows for cases
SELECT
  MIN(fulldate) AS min_date,
  MAX(fulldate) AS max_date,
  COUNT(*)      AS row_count
  FROM covid_silver_db.casesstandardized;

#-- Rows per state for testing
SELECT
  statecode,
  COUNT(*) AS row_count
FROM covid_silver_db.testingstandardized
GROUP BY statecode
ORDER BY row_count DESC;

#-- Basic null check
SELECT
  COUNT(*) AS missing_statecode
FROM covid_silver_db.casesstandardized
WHERE statecode IS NULL;
