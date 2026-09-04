-- Paid DAU from the GMIO feed, metric = 'Total Paid DAU' (view = total), country = 'All'.
-- Verbatim structure of the marketing team's widget query with the two template params resolved.
WITH view_pick AS (
    SELECT 'total' AS v
),
dau AS (
    SELECT
        d.week,
        d.channel,
        SUM(d.p50) AS v,
        SUM(d.pre_year) AS pre_year_v,
        LOGICAL_AND(NOT d.was_forecast) AS is_actual
    FROM `mozdata.analysis.ahe_gmio_weekly_paid_dau_views_20260901` AS d
    CROSS JOIN view_pick
    WHERE d.view = view_pick.v
        AND d.channel IN ('uac', 'meta_android')
        AND d.week >= DATE '2026-01-05'
    GROUP BY d.week, d.channel
),
series AS (
    SELECT week, channel, v, pre_year_v, is_actual FROM dau
),
meta_start AS (
    SELECT MIN(week) AS w
    FROM series
    WHERE channel = 'meta_android' AND (v - pre_year_v) > 0.5
),
by_channel AS (
    SELECT
        s.week,
        SUM(IF(s.channel = 'uac', s.v, 0)) AS uac_v,
        SUM(IF(s.channel = 'meta_android', s.v, 0)) AS meta_v,
        MAX(ms.w) IS NOT NULL AND s.week >= MAX(ms.w) AS has_meta,
        LOGICAL_AND(IF(s.channel = 'uac', s.is_actual, TRUE)) AS uac_actual_wk,
        LOGICAL_AND(IF(s.channel = 'meta_android', s.is_actual, TRUE)) AS meta_actual_wk
    FROM series AS s
    CROSS JOIN meta_start AS ms
    GROUP BY s.week
),
edges AS (
    SELECT
        MAX(IF(uac_actual_wk, week, NULL)) AS uac_edge,
        MAX(IF(has_meta AND meta_actual_wk, week, NULL)) AS meta_edge
    FROM by_channel
)
SELECT
    b.week AS date,
    ROUND(IF(b.week <= e.uac_edge, b.uac_v, NULL)) AS uac_actual,
    ROUND(IF(b.week >= e.uac_edge, b.uac_v, NULL)) AS uac_forecast,
    IF(b.has_meta AND b.week <= e.meta_edge, ROUND(b.uac_v + b.meta_v), NULL) AS uac_meta_actual,
    IF(b.has_meta AND b.week >= e.meta_edge, ROUND(b.uac_v + b.meta_v), NULL) AS uac_meta_forecast
FROM by_channel AS b
CROSS JOIN edges AS e
ORDER BY date
