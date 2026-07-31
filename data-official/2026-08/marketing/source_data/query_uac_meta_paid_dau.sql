-- August 2026 marketing lift source: UAC + Meta Android weekly Paid DAU, calendar 2026.
-- Template params resolved: {{metric}} = 'Total Paid DAU', {{country}} = 'All'.
-- Raw uac_v / meta_v components are exposed alongside the marketing-team
-- presentation columns so the "Meta is stacked (cumulative)" claim in the
-- original query's header comment can be verified rather than assumed.
WITH uac_cutoff AS (
    SELECT PARSE_DATE('%Y-%m-%d', REGEXP_EXTRACT(definition, r'\d{4}-\d{2}-\d{2}')) AS actuals_through
    FROM `mozdata.analysis.ahe_cmo_dashboard_notes_20260701`
    WHERE field = 'actuals_through'
),
meta_cutoff AS (
    SELECT PARSE_DATE('%Y-%m-%d', REGEXP_EXTRACT(definition, r'\d{4}-\d{2}-\d{2}')) AS actuals_through
    FROM `mozdata.analysis.ahe_meta_android_notes_20260721`
    WHERE field = 'actuals_through'
),
uac_total AS (
    SELECT week, SUM(paid_dau) AS v
    FROM `mozdata.analysis.ahe_cmo_dashboard_weekly_paid_dau_total_20260701`
    GROUP BY week
),
uac_rolling AS (
    SELECT week, SUM(paid_dau) AS v
    FROM `mozdata.analysis.ahe_cmo_dashboard_weekly_paid_dau_12mo_rolling_20260701`
    GROUP BY week
),
uac_np AS (
    SELECT week, SUM(new_profiles) AS v
    FROM `mozdata.analysis.ahe_cmo_dashboard_weekly_new_profiles_20260701`
    WHERE week >= DATE '2026-01-05' GROUP BY week
),
meta_pdau AS (
    SELECT week, SUM(paid_dau) AS v
    FROM `mozdata.analysis.ahe_meta_android_weekly_paid_dau_20260721`
    GROUP BY week
),
meta_np AS (
    SELECT week, SUM(new_profiles) AS v
    FROM `mozdata.analysis.ahe_meta_android_weekly_new_profiles_20260721`
    GROUP BY week
),
uac AS (
    SELECT week, t.v AS v, r.v AS v_rolling
    FROM uac_rolling r
    FULL OUTER JOIN uac_total t USING (week)
    FULL OUTER JOIN uac_np np USING (week)
),
meta AS (
    SELECT week, pd.v AS v
    FROM meta_pdau pd
    FULL OUTER JOIN meta_np np USING (week)
),
combined AS (
    SELECT week, uac.v AS uac_v, uac.v_rolling AS uac_v_rolling, meta.v AS meta_v
    FROM uac FULL OUTER JOIN meta USING (week)
)
SELECT
    week AS date,
    uac_k.actuals_through AS uac_actuals_through,
    meta_k.actuals_through AS meta_actuals_through,
    uac_v            AS uac_raw,
    uac_v_rolling    AS uac_raw_rolling_12mo,
    meta_v           AS meta_raw,
    ROUND(IF(week <= uac_k.actuals_through, uac_v, NULL)) AS uac_actual,
    ROUND(IF(week >= uac_k.actuals_through, uac_v, NULL)) AS uac_forecast,
    IF(meta_v IS NOT NULL AND week <= meta_k.actuals_through,
       ROUND(COALESCE(uac_v, 0) + meta_v), NULL) AS uac_meta_actual,
    IF(meta_v IS NOT NULL AND week >= meta_k.actuals_through,
       ROUND(COALESCE(uac_v, 0) + meta_v), NULL) AS uac_meta_forecast
FROM combined
CROSS JOIN uac_cutoff  uac_k
CROSS JOIN meta_cutoff meta_k
ORDER BY date
