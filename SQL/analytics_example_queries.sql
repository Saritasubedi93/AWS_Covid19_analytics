-- Top 5 states by new cases on a given date
SELECT
  s.statename,
  f.newcases
FROM covidgold.factcasesstatedaily f
JOIN covidgold.dimstate s
  ON s.statecode = f.statecode
WHERE f.dateid = 20200515
ORDER BY f.newcases DESC
LIMIT 5;

-- 7-day positivity rate trend for one state (example: NY)
SELECT
  d.fulldate,
  t.testsposcum::double precision / NULLIF(t.teststotalcum, 0) AS positivityrate
FROM covidgold.facttestingstatedaily t
JOIN covidgold.dimdate d
  ON d.dateid = t.dateid
WHERE t.statecode = 'NY'
ORDER BY d.fulldate;

-- Daily cases vs tests for a state (example: CA)
SELECT
  d.fulldate,
  c.newcases,
  t.newtests,
  t.testsposcum::double precision / NULLIF(t.teststotalcum, 0) AS positivityrate
FROM covidgold.factcasesstatedaily c
JOIN covidgold.facttestingstatedaily t
  ON t.dateid = c.dateid
 AND t.statecode = c.statecode
JOIN covidgold.dimdate d
  ON d.dateid = c.dateid
WHERE c.statecode = 'CA'
ORDER BY d.fulldate;

--start schema joining validation
SELECT d.fulldate,
       s.statename,
       f.newcases,
       t.newtests,
       t.positivityrate
FROM covid_gold.factcasesstatedaily f
LEFT JOIN covid_gold.dim_date d   ON f.dateid = d.dateid
LEFT JOIN covid_gold.dimstate s  ON f.statecode = s.statecode
LEFT JOIN covid_gold.facttestingstatedaily t
  ON f.dateid = t.dateid AND f.statecode = t.statecode
LIMIT 20;

--row-count check for multiple tables in one shot,
SELECT 'factcases' AS tbl, COUNT(*) FROM covid_gold.factcasesstatedaily
UNION ALL
SELECT 'facttesting', COUNT(*) FROM covid_gold.facttestingstatedaily
UNION ALL
SELECT 'dimdate', COUNT(*) FROM covid_gold.dim_date
UNION ALL
SELECT 'dimstate', COUNT(*) FROM covid_gold.dimstate;
