SELECT
  target_date,
  country,
  data_type,
  dau
FROM `moz-fx-data-shared-prod.forecasts_derived.mart_mozaic_daily_forecast_v2`
WHERE forecast_start_date = '2026-06-28'
  AND data_source = 'legacy_desktop'
  AND app_name = 'desktop'
  AND segment = '{"os": "ALL"}'
  AND country IN ('ALL','CN','US','IT','DE','FR','PL')
  AND target_date >= '2025-12-05'
ORDER BY country, target_date