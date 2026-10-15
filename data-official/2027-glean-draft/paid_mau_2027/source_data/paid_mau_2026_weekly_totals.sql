-- The same model's 2026 weekly paid DAU (portfolio grain, "total" view = all paid cohorts), run 2026-10-13: the bridge from our
-- data edge (2026-10-09) to the 2027 plan. Weeks through the data edge carry `measured`; later weeks are forecast (was_forecast).
SELECT CAST(week AS STRING) AS week, view, p50, low_90, q_alpha, was_forecast, measured, ci_level, low_pct, alpha
FROM `mozdata.analysis.ahe_gmio_weekly_paid_dau_totals_20261013`
WHERE view = 'total'
ORDER BY week
