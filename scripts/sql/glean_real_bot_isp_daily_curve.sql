-- Daily bot-like component of real Glean desktop DAU and MAU, per country x OS, measured from real Glean alone.
-- Definition (Brendan, 2026-10-14, "go with the ISP definition"): a client is bot-like on a day if its ISP's behavioural
-- share that day is >= 0.8, where the behavioural share = fraction of the ISP's MAU clients that were first seen inside the
-- 28-day window AND had <= 2 active days in it. Hosting / cloud providers come out at 0.95-1.00, residential ISPs ~0.3,
-- so the threshold is a wide gap, and hosting is an outcome rather than a hand-curated list. ISPs with fewer than 50
-- sampled MAU clients (500 population) on the day are never flagged, so tiny ISPs cannot flip on noise.
-- Source: firefox_desktop.baseline_active_users (client level; the aggregates' own is_dau / is_mau), Firefox Desktop,
-- 10% sample (sample_id 0-9) scaled x10. Country bucketing as mozaic-daily's production query (15 named markets + ROW).
WITH clients AS (
  SELECT
    submission_date,
    IF(country IN ('AR','BR','CA','CN','DE','FR','ID','IN','IR','IT','JP','MX','PL','RU','US'), country, 'ROW') AS country,
    CASE os_grouped WHEN 'Windows' THEN 'windows' WHEN 'Mac' THEN 'mac' WHEN 'Linux' THEN 'linux' END AS os_bucket,
    COALESCE(isp, '(null)') AS isp,
    is_dau,
    is_mau,
    first_seen_date >= DATE_SUB(submission_date, INTERVAL 27 DAY) AND BIT_COUNT(days_active_bits) <= 2 AS behavioural
  FROM `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
  WHERE submission_date BETWEEN '2025-05-22' AND '2026-10-09'
    AND sample_id BETWEEN 0 AND 9
    AND app_name = 'Firefox Desktop'
    AND os_grouped IN ('Windows', 'Mac', 'Linux')
),
isp_day AS (
  SELECT submission_date, os_bucket, isp,
         COUNTIF(is_mau) AS mau_clients,
         SAFE_DIVIDE(COUNTIF(is_mau AND behavioural), COUNTIF(is_mau)) AS behavioural_share
  FROM clients
  GROUP BY ALL
),
flagged AS (
  SELECT submission_date, os_bucket, isp, (mau_clients >= 50 AND behavioural_share >= 0.8) AS bot_isp
  FROM isp_day
)
SELECT
  c.submission_date AS x,
  c.country,
  c.os_bucket,
  10 * COUNTIF(c.is_dau) AS dau,
  10 * COUNTIF(c.is_mau) AS mau,
  10 * COUNTIF(c.is_dau AND f.bot_isp) AS dau_bot,
  10 * COUNTIF(c.is_mau AND f.bot_isp) AS mau_bot,
  10 * COUNTIF(c.is_mau AND c.behavioural) AS mau_behavioural
FROM clients c
JOIN flagged f USING (submission_date, os_bucket, isp)
GROUP BY ALL
ORDER BY 1, 2, 3
