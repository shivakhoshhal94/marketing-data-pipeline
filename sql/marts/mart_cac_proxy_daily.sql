CREATE SCHEMA IF NOT EXISTS marts;
DROP TABLE IF EXISTS marts.cac_proxy_daily;
CREATE TABLE marts.cac_proxy_daily AS
SELECT
  f.date,
  f.source,
  a.spend,
  f.signups,
  CASE WHEN f.signups>0 THEN a.spend / f.signups END AS cac_proxy
FROM marts.funnel_daily f
LEFT JOIN staging.stg_adspend a
  ON a.date=f.date AND a.source=f.source
ORDER BY 1,2;
