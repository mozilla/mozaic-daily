-- Fenix paid / organic split of rolling-28 MAU by country x day, for the 2027 ORGANIC mobile MAU forecast
-- (Brendan, 2026-10-15: "we only forecast the organic part of mobile; marketing forecasts paid").
--
-- Definitionally identical to the DAU split behind adjustment `p` (fenix_growth_source_tail.sql): a client is paid iff
-- mozdata.fenix.new_profile_clients has paid_vs_organic_gclid = 'Paid' AND release channel AND Play Store install;
-- everything else is organic by residual. The only change is `is_mau` in place of `is_dau`, so the numerator and
-- denominator are 28-day active clients. Same country bucketing as production (top-DAU markets named, else ROW).
-- mozdata.fenix.active_users retains ~25 months, so the series starts where the view does; the runner holds the
-- earliest measured share flat backwards, as `p` does for DAU.
WITH paid_clients AS (
  SELECT client_id, MIN(first_seen_date) AS first_seen_date
  FROM `mozdata.fenix.new_profile_clients`
  WHERE paid_vs_organic_gclid = 'Paid'
    AND normalized_channel = 'release'
    AND install_source = 'com.android.vending'
    AND first_seen_date <= DATE '2026-10-09'
  GROUP BY client_id
),

au AS (
  SELECT
    submission_date,
    IFNULL(country, '??') AS country,
    client_id
  FROM `mozdata.fenix.active_users`
  WHERE is_mau
    AND app_name = 'Fenix'
    AND submission_date BETWEEN DATE '2024-06-01' AND DATE '2026-10-09'
)

SELECT
  au.submission_date,
  IF(au.country IN ('AR','BR','CA','CN','DE','FR','ID','IN','IR','IT','JP','MX','PL','RU','US'), au.country, 'ROW') AS country,
  CASE
    WHEN pc.client_id IS NULL THEN 'organic'
    WHEN pc.first_seen_date >= DATE_SUB(au.submission_date, INTERVAL 365 DAY)
      THEN 'paid_rolling_12mo'
    ELSE 'paid_prior_1yr'
  END AS growth_source,
  COUNT(*) AS mau
FROM au
LEFT JOIN paid_clients AS pc
  USING (client_id)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
