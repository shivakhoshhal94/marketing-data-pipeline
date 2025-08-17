CREATE SCHEMA IF NOT EXISTS staging;
DROP TABLE IF EXISTS staging.stg_events;
CREATE TABLE staging.stg_events AS
SELECT
  (event_time)::timestamp AS event_time,
  date_trunc('day', event_time)::date AS date,
  user_id::bigint AS user_id,
  session_id::text AS session_id,
  source::text AS source,
  campaign::text AS campaign,
  geo::text AS geo,
  action::text AS action,
  CASE WHEN action='signup' THEN 1 ELSE 0 END AS is_signup,
  CASE WHEN action='purchase' THEN 1 ELSE 0 END AS is_purchase
FROM public.raw_events;
