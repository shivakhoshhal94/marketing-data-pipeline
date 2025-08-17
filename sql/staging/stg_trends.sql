CREATE SCHEMA IF NOT EXISTS staging;
DROP TABLE IF EXISTS staging.stg_trends;
CREATE TABLE staging.stg_trends AS
SELECT
  date::date AS date,
  keyword::text AS keyword,
  geo::text AS geo,
  value::int AS value
FROM public.raw_trends
WHERE value IS NOT NULL;
