-- Real Glean desktop DAU and MAU by country x OS from the clean overlap start (2025-05-22, legacy-to-glean's definition),
-- for the 2027 Glean-history comparison forecast. Same bucketing as the Legacy pull.
SELECT
  submission_date AS x,
  IF(country IN ('AR','BR','CA','CN','DE','FR','ID','IN','IR','IT','JP','MX','PL','RU','US'), country, 'ROW') AS country,
  CASE
    WHEN LOWER(os) LIKE '%windows%' THEN 'windows'
    WHEN LOWER(os) LIKE '%mac%' THEN 'mac'
    WHEN LOWER(os) LIKE '%linux%' THEN 'linux'
    ELSE 'other'
  END AS os_bucket,
  SUM(dau) AS dau,
  SUM(mau) AS mau
FROM `moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates`
WHERE app_name = 'Firefox Desktop'
  AND submission_date >= '2025-05-22'
  AND submission_date <= '2026-10-09'
GROUP BY ALL
ORDER BY 1, 2, 3
