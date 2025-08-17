CREATE SCHEMA IF NOT EXISTS staging;
DROP TABLE IF EXISTS staging.stg_adspend;
CREATE TABLE staging.stg_adspend AS
SELECT
  date::date AS date,
  source::text AS source,
  spend::numeric(12,2) AS spend
FROM public.raw_adspend;
