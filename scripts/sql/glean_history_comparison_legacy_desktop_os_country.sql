-- Legacy desktop DAU and MAU by country x OS, 2020-01-01 onward, for the 2027 Glean-history comparison forecast.
-- Same country bucketing as mozaic-daily's production query (15 named markets + ROW); OS folded to windows/mac/linux.
SELECT
  submission_date AS x,
  IF(country IN ('AR','BR','CA','CN','DE','FR','ID','IN','IR','IT','JP','MX','PL','RU','US'), country, 'ROW') AS country,
  CASE
    WHEN LOWER(os) LIKE '%windows%' THEN 'windows'
    WHEN LOWER(os) LIKE '%mac%' OR LOWER(os) LIKE '%darwin%' THEN 'mac'   -- Legacy reports macOS as 'Darwin'
    WHEN LOWER(os) LIKE '%linux%' THEN 'linux'
    ELSE 'other'
  END AS os_bucket,
  SUM(dau) AS dau,
  SUM(mau) AS mau
FROM `moz-fx-data-shared-prod.telemetry.active_users_aggregates`
WHERE app_name = 'Firefox Desktop'
  AND submission_date >= '2020-01-01'
  AND submission_date <= '2026-10-09'
GROUP BY ALL
ORDER BY 1, 2, 3
