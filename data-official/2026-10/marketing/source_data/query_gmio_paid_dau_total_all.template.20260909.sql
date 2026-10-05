-- Paid DAU / attributed new profiles from the Growth Marketing Investment Optimizer feed.
-- {{metric}} picks the series; the chart draws four lines: UAC actual/forecast and
-- UAC+Meta-Android actual/forecast. Meta is STACKED on top of UAC by emitting cumulative
-- values, so the Meta line rides on the UAC line and the band between them is Meta's
-- contribution. Actual and forecast overlap by one week at the handoff so the lines connect.
--
-- SOURCE CHANGE (see notes table): this used to read ahe_cmo_dashboard_* (UAC only) and
-- ahe_meta_android_* (Meta only). It now reads one cross-channel feed,
-- ahe_gmio_weekly_paid_dau_views / ahe_gmio_weekly_metrics, which carries all 12 paid
-- channels, credible bounds on every row, and a per-row was_forecast flag.
--
-- THE NUMBERS MOVE, and the move is real rather than a definitional artefact. The metric
-- definitions are unchanged -- the old feed's own notes define paid_dau_12mo_rolling as
-- "cohorts aged <= 52 weeks, any acquisition date", which is exactly the rolling_12mo view
-- here. What changed is the forecast behind it: future UAC spend went from $4.75M over 19
-- weeks to $6.14M over 18 weeks (+36%/wk), and the curves were refit (GMIO run 2026-08-28,
-- carrying the install-week and edge-week retention corrections). Year-end UAC rolling paid
-- DAU therefore reads ~1.74M where the old feed read ~1.20M. Elapsed weeks move only 3-6%,
-- which is the actuals revision; the rest is the plan and the refit.
--
-- Three things that changed shape, and why the SQL looks different:
--   1. `was_forecast` is a COLUMN now, not a date parsed out of a notes table. One less
--      cross join and no regex, and it is per row rather than per channel.
--   2. Paid DAU carries a `view`: current_year | total | rolling_12mo. {{metric}} selects
--      it directly instead of choosing between three separate tables.
--   3. Both channels come from the same table, so the UAC/Meta split is a WHERE clause.
--      Adding a channel to the chart is now a filter change, not a new table.
--
-- The interval columns (p025/p975/q_alpha) exist in the source and are deliberately NOT
-- charted here: this widget is a level chart, and per-cell bounds must never be summed
-- across countries (quantiles do not add). Use ahe_gmio_weekly_paid_dau_totals for a
-- banded portfolio view -- it quantiled within the draw.
WITH view_pick AS (
    SELECT CASE '{{metric}}'
        WHEN 'Total Paid DAU' THEN 'total'
        WHEN '2026-acquired Paid DAU' THEN 'current_year'
        ELSE 'rolling_12mo'
    END AS v
),
dau AS (
    SELECT
        d.week,
        d.channel,
        SUM(d.p50) AS v,
        -- Carried so the Meta line can start at Meta's LAUNCH rather than at its first row:
        -- the pre-2026 cohort is a trace (~6 DAU on fb4a/ig4a-tagged installs that predate
        -- the paid test) and must not be read as the channel having run.
        SUM(d.pre_year) AS pre_year_v,
        -- A week is actual only where EVERY contributing row is actual. Any forecast row
        -- makes the aggregate a forecast; averaging the flag would invent a half-actual week.
        LOGICAL_AND(NOT d.was_forecast) AS is_actual
    FROM `mozdata.analysis.ahe_gmio_weekly_paid_dau_views_20260909` AS d
    CROSS JOIN view_pick
    WHERE d.view = view_pick.v
        AND d.channel IN ('uac', 'meta_android')
        AND ('{{country}}' = 'All' OR d.country = '{{country}}')
        -- Same lower bound as the new-profiles branch below. The feed's calendar is ISO, so
        -- it opens on 2025-12-29 (ISO week 1 of 2026); without this the x-axis SHIFTED BY A
        -- WEEK when {{metric}} toggled between a DAU view and Attributed New Profiles.
        AND d.week >= DATE '2026-01-05'
    GROUP BY d.week, d.channel
),
np AS (
    SELECT
        m.week,
        m.channel,
        SUM(m.p50) AS v,
        0.0 AS pre_year_v,   -- new profiles are a flow: no pre-year stock to carry
        LOGICAL_AND(NOT m.was_forecast) AS is_actual
    FROM `mozdata.analysis.ahe_gmio_weekly_metrics_20260909` AS m
    WHERE m.metric = 'new_profiles'
        AND m.channel IN ('uac', 'meta_android')
        AND ('{{country}}' = 'All' OR m.country = '{{country}}')
        AND m.week >= DATE '2026-01-05'
    GROUP BY m.week, m.channel
),
series AS (
    SELECT week, channel, v, pre_year_v, is_actual FROM dau
    WHERE '{{metric}}' != 'Attributed New Profiles'
    UNION ALL
    SELECT week, channel, v, pre_year_v, is_actual FROM np
    WHERE '{{metric}}' = 'Attributed New Profiles'
),
meta_start AS (
    -- Meta Android's launch, derived rather than hardcoded: the first week its CURRENT-YEAR
    -- contribution is non-trivial. Lands on 2026-05-04, the ISO Monday of the documented
    -- 2026-05-08 launch. NULL if Meta never runs in the selected country, which correctly
    -- suppresses the stacked lines entirely.
    SELECT MIN(week) AS w
    FROM series
    WHERE channel = 'meta_android' AND (v - pre_year_v) > 0.5
),
by_channel AS (
    SELECT
        s.week,
        SUM(IF(s.channel = 'uac', s.v, 0)) AS uac_v,
        SUM(IF(s.channel = 'meta_android', s.v, 0)) AS meta_v,
        -- Meta counts as present only from its launch week onward.
        MAX(ms.w) IS NOT NULL AND s.week >= MAX(ms.w) AS has_meta,
        LOGICAL_AND(IF(s.channel = 'uac', s.is_actual, TRUE)) AS uac_actual_wk,
        LOGICAL_AND(IF(s.channel = 'meta_android', s.is_actual, TRUE)) AS meta_actual_wk
    FROM series AS s
    CROSS JOIN meta_start AS ms
    GROUP BY s.week
),
edges AS (
    -- The handoff week for each channel: its last actual week. Emitting it on BOTH the
    -- actual and the forecast line is what closes the visual gap at the join.
    SELECT
        MAX(IF(uac_actual_wk, week, NULL)) AS uac_edge,
        MAX(IF(has_meta AND meta_actual_wk, week, NULL)) AS meta_edge
    FROM by_channel
)
SELECT
    b.week AS date,
    ROUND(IF(b.week <= e.uac_edge, b.uac_v, NULL)) AS uac_actual,
    ROUND(IF(b.week >= e.uac_edge, b.uac_v, NULL)) AS uac_forecast,
    -- Cumulative UAC + Meta (hence the uac_meta_ prefix); stacks on the UAC-only lines.
    IF(b.has_meta AND b.week <= e.meta_edge, ROUND(b.uac_v + b.meta_v), NULL)
        AS uac_meta_actual,
    IF(b.has_meta AND b.week >= e.meta_edge, ROUND(b.uac_v + b.meta_v), NULL)
        AS uac_meta_forecast
FROM by_channel AS b
CROSS JOIN edges AS e
ORDER BY date
