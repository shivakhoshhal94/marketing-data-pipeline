CREATE SCHEMA IF NOT EXISTS marts;
DROP TABLE IF EXISTS marts.geo_keyword_daily;
CREATE TABLE marts.geo_keyword_daily AS
SELECT
  t.date,
  t.geo,
  t.keyword,
  t.value,
  AVG(t.value) OVER (PARTITION BY t.keyword ORDER BY t.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS value_7d_ma
FROM staging.stg_trends t
ORDER BY 1,2,3;
