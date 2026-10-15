-- Marketing DS (ahe) 2027 paid MAU plan for Fenix (UAC + Meta Android, world), daily, both scenarios, attributed basis.
-- Behind STMO query 128941 and the quick site ahe-2027-planning-first-scenario. MAU = weekly paid DAU x 4.792 x day-of-week factor.
SELECT
  CAST(date AS STRING) AS date, CAST(week AS STRING) AS week, day_of_week, day_ratio,
  mau, mau_low_90, mau_high_90, mau_same_year, mau_same_year_low_90, mau_same_year_high_90,
  mau_per_dau, scenario, scenario_label, channels, CAST(curve_run AS STRING) AS curve_run, ci_level, n_draws, seed,
  CAST(generated_at AS STRING) AS generated_at
FROM `mozdata.analysis.ahe_gmio_plan2027_mau_daily_20261014`
ORDER BY scenario, date
