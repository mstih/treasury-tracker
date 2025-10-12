-- recompute_aggregates.sql
BEGIN;

TRUNCATE TABLE tariff_monthly;
INSERT INTO tariff_monthly (month, tariff_millions_sum, total_deposits_millions_sum, updated_at)
SELECT
  date_trunc('month', dts_date)::date AS month,
  COALESCE(SUM(tariff_millions), 0) AS tariff_millions_sum,
  COALESCE(SUM(total_deposits_millions), 0) AS total_deposits_millions_sum,
  NOW() AS updated_at
FROM tariff_daily
GROUP BY month
ORDER BY month;

TRUNCATE TABLE tariff_yearly;
INSERT INTO tariff_yearly (year, tariff_millions_sum, total_deposits_millions_sum, updated_at)
SELECT
  EXTRACT(YEAR FROM dts_date)::int AS year,
  COALESCE(SUM(tariff_millions), 0) AS tariff_millions_sum,
  COALESCE(SUM(total_deposits_millions), 0) AS total_deposits_millions_sum,
  NOW() AS updated_at
FROM tariff_daily
GROUP BY year
ORDER BY year;

COMMIT;
