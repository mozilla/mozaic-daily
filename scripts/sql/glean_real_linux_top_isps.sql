-- Linux desktop, real Glean, client level: top ISPs by MAU in DE, US and ROW on two snapshot days (before and inside the surge),
-- with the behavioural bot-like share (first seen inside the window AND at most two active days) per ISP. 10% sample, scaled x10.
WITH snap AS (
  SELECT
    submission_date,
    IF(country IN ('AR','BR','CA','CN','DE','FR','ID','IN','IR','IT','JP','MX','PL','RU','US'), country, 'ROW') AS country,
    COALESCE(isp, '(null)') AS isp,
    is_mau,
    first_seen_date >= DATE_SUB(submission_date, INTERVAL 27 DAY) AND BIT_COUNT(days_active_bits) <= 2 AS bot_like
  FROM `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
  WHERE submission_date IN ('2025-06-15', '2026-10-09')
    AND sample_id BETWEEN 0 AND 9
    AND os_grouped = 'Linux'
    AND app_name = 'Firefox Desktop'
    AND (country IN ('DE', 'US') OR country NOT IN ('AR','BR','CA','CN','FR','ID','IN','IR','IT','JP','MX','PL','RU'))
),
by_isp AS (
  SELECT submission_date, country, isp,
         10 * COUNTIF(is_mau) AS mau,
         10 * COUNTIF(is_mau AND bot_like) AS mau_bot_like
  FROM snap
  GROUP BY ALL
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY submission_date, country ORDER BY mau DESC) AS rk,
         SUM(mau) OVER (PARTITION BY submission_date, country) AS country_mau
  FROM by_isp
)
SELECT submission_date, country, rk, isp, mau, mau_bot_like, ROUND(mau / country_mau, 4) AS share_of_country_mau
FROM ranked
WHERE rk <= 15
ORDER BY submission_date, country, rk
