-- Allocation shares for adjustment `e` (launch at login for existing users):
-- each country's share of trailing-28d legacy-desktop DAU on Windows 10/11 in an en locale.
-- Same table, OS bucketing (LOWER(os_version) LIKE '%windows 1%') and country folding (top markets, else ROW)
-- as the production legacy_desktop DAU query in src/mozaic_daily/queries.py; window ends training_end 2026-09-08.
WITH base AS (
  SELECT
    IF(country IN ('US','BR','CA','MX','AR','IN','ID','JP','CN','DE','FR','PL','RU','IT','IR'), country, 'ROW') AS country,
    SUM(dau) AS dau
  FROM `moz-fx-data-shared-prod.telemetry.active_users_aggregates`
  WHERE app_name = 'Firefox Desktop'
    AND submission_date BETWEEN '2026-08-12' AND '2026-09-08'
    AND IFNULL(LOWER(os_version) LIKE '%windows 1%', FALSE)
    AND LOWER(locale) LIKE 'en%'
  GROUP BY 1
)
SELECT country, dau, ROUND(dau / SUM(dau) OVER (), 6) AS share
FROM base
ORDER BY dau DESC
