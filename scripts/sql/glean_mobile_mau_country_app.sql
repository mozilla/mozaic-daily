-- Mobile DAU and MAU by country x app from the production mobile training start (2020-12-31), for the 2027 mobile MAU
-- forecast beside the desktop Glean-history draft (data-official/2027-glean-draft/). Same universe, bucketing and
-- table as the production mobile DAU QuerySpec (queries.py): four apps, top-DAU markets named, everything else ROW.
-- Mobile is Glean-native, so this is a full real history with no translation.
SELECT
  submission_date AS x,
  IF(country IN ('AR','BR','CA','CN','DE','FR','ID','IN','IR','IT','JP','MX','PL','RU','US'), country, 'ROW') AS country,
  app_name,
  SUM(dau) AS dau,
  SUM(mau) AS mau
FROM `moz-fx-data-shared-prod.glean_telemetry.active_users_aggregates`
WHERE app_name IN ('Fenix', 'Firefox iOS', 'Focus Android', 'Focus iOS')
  AND submission_date >= '2020-12-31'
  AND submission_date <= '2026-10-09'
GROUP BY ALL
ORDER BY 1, 2, 3
