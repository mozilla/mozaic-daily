-- Reference only: the marketing model's weekly paid DAU by country ("total" view), run 2026-10-13. Our per-country allocation of
-- paid MAU uses OUR measured split's trailing-28-day share (decision 2026-10-15), not this table; it is kept beside for comparison.
SELECT CAST(week AS STRING) AS week, country, p50, low_90, was_forecast, measured
FROM `mozdata.analysis.ahe_gmio_weekly_paid_dau_by_country_20261013`
WHERE view = 'total'
ORDER BY week, country
