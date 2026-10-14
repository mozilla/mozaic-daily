-- Linux desktop, real Glean, client level (firefox_desktop.baseline_active_users; same is_dau / is_mau flags as the aggregates).
-- Behavioural bot-like segment: first seen inside the 28-day window AND at most two active days in it.
-- 10% sample (sample_id 0-9); counts are scaled x10 to the population. Snapshot days: 1st and 15th of each month.
-- Country bucketing as mozaic-daily's production query (15 named markets + ROW).
WITH snap AS (
  SELECT
    submission_date,
    IF(country IN ('AR','BR','CA','CN','DE','FR','ID','IN','IR','IT','JP','MX','PL','RU','US'), country, 'ROW') AS country,
    is_dau,
    is_mau,
    first_seen_date >= DATE_SUB(submission_date, INTERVAL 27 DAY) AS is_new_28d,
    BIT_COUNT(days_active_bits) <= 2 AS low_activity
  FROM `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
  WHERE submission_date BETWEEN '2025-06-01' AND '2026-10-09'
    AND (EXTRACT(DAY FROM submission_date) IN (1, 15) OR submission_date = '2026-10-09')
    AND sample_id BETWEEN 0 AND 9
    AND os_grouped = 'Linux'
    AND app_name = 'Firefox Desktop'
)
SELECT
  submission_date,
  country,
  10 * COUNTIF(is_mau) AS mau,
  10 * COUNTIF(is_mau AND is_new_28d AND low_activity) AS mau_new_lowact,
  10 * COUNTIF(is_mau AND is_new_28d) AS mau_new,
  10 * COUNTIF(is_mau AND low_activity) AS mau_lowact,
  10 * COUNTIF(is_dau) AS dau,
  10 * COUNTIF(is_dau AND is_new_28d AND low_activity) AS dau_new_lowact
FROM snap
GROUP BY ALL
ORDER BY 1, 2
