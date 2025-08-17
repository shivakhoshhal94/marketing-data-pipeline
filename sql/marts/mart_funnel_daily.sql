CREATE SCHEMA IF NOT EXISTS marts;
DROP TABLE IF EXISTS marts.funnel_daily;
CREATE TABLE marts.funnel_daily AS
WITH sessions AS (
  SELECT date, source, COUNT(DISTINCT session_id) AS sessions
  FROM staging.stg_events
  GROUP BY 1,2
),
signups AS (
  SELECT date, source, COUNT(*) AS signups
  FROM staging.stg_events
  WHERE is_signup=1
  GROUP BY 1,2
),
purchases AS (
  SELECT date, source, COUNT(*) AS purchases
  FROM staging.stg_events
  WHERE is_purchase=1
  GROUP BY 1,2
)
SELECT
  COALESCE(s.date, si.date, p.date) AS date,
  COALESCE(s.source, si.source, p.source) AS source,
  COALESCE(s.sessions, 0) AS sessions,
  COALESCE(si.signups, 0) AS signups,
  COALESCE(p.purchases, 0) AS purchases,
  CASE WHEN COALESCE(s.sessions,0)>0 THEN COALESCE(si.signups,0)::float / NULLIF(COALESCE(s.sessions,0),0) END AS signup_rate,
  CASE WHEN COALESCE(si.signups,0)>0 THEN COALESCE(p.purchases,0)::float / NULLIF(COALESCE(si.signups,0),0) END AS purchase_rate
FROM sessions s
FULL OUTER JOIN signups si USING (date, source)
FULL OUTER JOIN purchases p USING (date, source)
ORDER BY 1,2;
