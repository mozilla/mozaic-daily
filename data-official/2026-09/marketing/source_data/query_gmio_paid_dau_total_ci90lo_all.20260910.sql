-- Resolved by scripts/pull_paid_dau_curve.py on 2026-09-10: {"metric": "Total Paid DAU", "country": "All"}
-- Paid DAU / attributed new profiles from the Growth Marketing Investment Optimizer feed.
-- Total Paid DAU picks the series; the chart draws four lines: UAC actual/forecast and
-- UAC+Meta-Android actual/forecast. Meta is STACKED on top of UAC, so the Meta line rides
-- on the UAC line and the band between them is Meta's contribution. Actual and forecast
-- overlap by one week at the handoff so the lines connect.
--
-- ============================================================================
-- THIS QUERY IS THE LOWER-BOUND (90% CL) VIEW.
--   value = the lower end of the 90% credible interval (ci_lo = p5) on forecast weeks,
--           the measured actual before that
-- Its twin is query 122861 (point estimate). The two are IDENTICAL on actualized weeks
-- and diverge only in the forecast. See "actualized weeks" below for why that took work.
-- ============================================================================
--
-- ACTUALIZED WEEKS READ `measured`, AND THAT IS THE POINT.
-- The modelled columns carry a band on EVERY week, past included: the projection
-- substitutes observed new profiles and activations on elapsed weeks, but paid DAU is then
-- DERIVED from them through the FITTED retention curve, so a past week inherits the
-- retention posterior (~1.35% at p5). That is why this chart and its twin used to disagree
-- about a week the axis labels "actual" -- one showed the modelled median, the other the
-- modelled 5th percentile. Neither was a measurement.
-- The feed now carries `measured`, the telemetry series at each table's own grain, NULL on
-- forecast weeks and byte-identical in both feeds. So COALESCE(measured, ci_lo) is a
-- genuine actual up to the data edge and the modelled value after it.
--
-- QUANTILES DO NOT ADD, SO THE SOURCE TABLE DEPENDS ON All.
-- A band is only valid at the grain it was quantiled at (hard rule 11). Summing the
-- per-cell lower bound across countries under-stated the portfolio floor by 2.5% on the
-- live run, against 0.10% for the median -- so the median feed survived the mistake and
-- the lower-bound feed did not, which presented as the two feeds disagreeing about the
-- PAST rather than as an aggregation error. The feed therefore publishes paid DAU at four
-- grains, each aggregated WITHIN the draw, and this query reads whichever one matches the
-- filter instead of summing a finer one:
--
--   All = 'All'   UAC line      -> ..._by_country is wrong; use ..._by_channel
--                       UAC+Meta line -> ..._totals  (the portfolio IS uac + meta_android;
--                                        verified -- those are the only two channels in
--                                        the feed. Adding a desktop channel to the run
--                                        would break this and the line would silently
--                                        include it.)
--   All = 'XX'    UAC line      -> ..._views, one row per (channel, country):
--                                        the cell grain already IS within-draw there
--                       UAC+Meta line -> ..._by_country, which summed the two channels
--                                        inside the draw
--
-- THE ACTUAL LINE ENDS WHERE THE MEASUREMENT ENDS, NOT WHERE THE SPEND CALENDAR DOES.
-- On the DAU branch a week counts as actual iff `measured IS NOT NULL`, not iff
-- `was_forecast = FALSE`. Those differ by exactly one week: retention cells are only
-- emitted once their 7-day window has closed for EVERY client in the cohort, so the
-- measured paid-DAU series ends one week before the spend calendar's data edge. On the
-- live run that is 2026-08-31 -- flagged actual by the calendar, with no closed retention
-- measurement behind it. Charting it on the actual line would put a MODELLED value there,
-- which is the one week the two feeds would still disagree about. It goes on the forecast
-- line instead, where a difference between the median and the p5 is the point.
-- The new-profiles branch keeps `was_forecast`, because there the observed override IS the
-- measurement: spend and conversions are complete for that week.
--
-- The new-profiles branch still sums across countries, and that is deliberate: on elapsed
-- weeks new profiles are the OBSERVED values with zero variance -- verified, the two feeds
-- agree to 0 across all 2,088 actual rows -- so the summing error touches the FORECAST
-- portion only. There is no per-channel within-draw table for new profiles to read instead.
WITH view_pick AS (
    SELECT CASE 'Total Paid DAU'
        WHEN 'Total Paid DAU' THEN 'total'
        WHEN '2026-acquired Paid DAU' THEN 'current_year'
        ELSE 'rolling_12mo'
    END AS v
),
-- ---- paid DAU, read at the grain All calls for ----------------------------
uac_dau AS (
    SELECT b.week, COALESCE(b.measured, b.ci_lo) AS v, b.measured IS NOT NULL AS is_actual
    FROM `mozdata.analysis.ahe_gmio_weekly_paid_dau_by_channel_ci90_20260909` AS b
    CROSS JOIN view_pick AS vp
    WHERE 'All' = 'All' AND b.channel = 'uac' AND b.view = vp.v
        -- The feed's calendar is ISO and opens on 2025-12-29 (ISO week 1 of 2026). Without
        -- this bound the x-axis SHIFTED BY A WEEK when Total Paid DAU toggled to New Profiles.
        AND b.week >= DATE '2026-01-05'
    UNION ALL
    SELECT d.week, COALESCE(d.measured, d.ci_lo), d.measured IS NOT NULL
    FROM `mozdata.analysis.ahe_gmio_weekly_paid_dau_views_ci90_20260909` AS d
    CROSS JOIN view_pick AS vp
    WHERE 'All' != 'All' AND d.channel = 'uac' AND d.country = 'All'
        AND d.view = vp.v AND d.week >= DATE '2026-01-05'
),
both_dau AS (
    SELECT t.week, COALESCE(t.measured, t.ci_lo) AS v, t.measured IS NOT NULL AS is_actual
    FROM `mozdata.analysis.ahe_gmio_weekly_paid_dau_totals_ci90_20260909` AS t
    CROSS JOIN view_pick AS vp
    WHERE 'All' = 'All' AND t.view = vp.v AND t.week >= DATE '2026-01-05'
    UNION ALL
    SELECT c.week, COALESCE(c.measured, c.ci_lo), c.measured IS NOT NULL
    FROM `mozdata.analysis.ahe_gmio_weekly_paid_dau_by_country_ci90_20260909` AS c
    CROSS JOIN view_pick AS vp
    WHERE 'All' != 'All' AND c.country = 'All'
        AND c.view = vp.v AND c.week >= DATE '2026-01-05'
),
-- Meta's CURRENT-YEAR paid DAU, used only to find its launch week. The current_year view
-- excludes the pre-2026 cohort, which on Meta is a trace (~6 DAU on fb4a/ig4a-tagged
-- installs predating the paid test) and must not be read as the channel having run.
meta_cy AS (
    SELECT b.week, COALESCE(b.measured, b.ci_lo) AS v
    FROM `mozdata.analysis.ahe_gmio_weekly_paid_dau_by_channel_ci90_20260909` AS b
    WHERE 'All' = 'All' AND b.channel = 'meta_android'
        AND b.view = 'current_year' AND b.week >= DATE '2026-01-05'
    UNION ALL
    SELECT d.week, COALESCE(d.measured, d.ci_lo)
    FROM `mozdata.analysis.ahe_gmio_weekly_paid_dau_views_ci90_20260909` AS d
    WHERE 'All' != 'All' AND d.channel = 'meta_android'
        AND d.country = 'All' AND d.view = 'current_year'
        AND d.week >= DATE '2026-01-05'
),
-- ---- attributed new profiles ---------------------------------------------------------
uac_np AS (
    SELECT m.week, SUM(m.ci_lo) AS v, LOGICAL_AND(NOT m.was_forecast) AS is_actual
    FROM `mozdata.analysis.ahe_gmio_weekly_metrics_ci90_20260909` AS m
    WHERE m.metric = 'new_profiles' AND m.channel = 'uac'
        AND ('All' = 'All' OR m.country = 'All')
        AND m.week >= DATE '2026-01-05'
    GROUP BY m.week
),
both_np AS (
    SELECT m.week, SUM(m.ci_lo) AS v, LOGICAL_AND(NOT m.was_forecast) AS is_actual
    FROM `mozdata.analysis.ahe_gmio_weekly_metrics_ci90_20260909` AS m
    WHERE m.metric = 'new_profiles' AND m.channel IN ('uac', 'meta_android')
        AND ('All' = 'All' OR m.country = 'All')
        AND m.week >= DATE '2026-01-05'
    GROUP BY m.week
),
meta_np AS (
    SELECT m.week, SUM(m.ci_lo) AS v
    FROM `mozdata.analysis.ahe_gmio_weekly_metrics_ci90_20260909` AS m
    WHERE m.metric = 'new_profiles' AND m.channel = 'meta_android'
        AND ('All' = 'All' OR m.country = 'All')
        AND m.week >= DATE '2026-01-05'
    GROUP BY m.week
),
-- ---- pick the metric -----------------------------------------------------------------
-- Grouped defensively: each source above is already one row per week, and a duplicate
-- would otherwise double a line rather than error.
uac AS (
    SELECT week, SUM(v) AS v, LOGICAL_AND(is_actual) AS is_actual
    FROM (SELECT * FROM uac_dau WHERE 'Total Paid DAU' != 'Attributed New Profiles'
          UNION ALL
          SELECT * FROM uac_np WHERE 'Total Paid DAU' = 'Attributed New Profiles')
    GROUP BY week
),
both AS (
    SELECT week, SUM(v) AS v, LOGICAL_AND(is_actual) AS is_actual
    FROM (SELECT * FROM both_dau WHERE 'Total Paid DAU' != 'Attributed New Profiles'
          UNION ALL
          SELECT * FROM both_np WHERE 'Total Paid DAU' = 'Attributed New Profiles')
    GROUP BY week
),
meta_start AS (
    -- Meta's launch, derived rather than hardcoded: the first week its own contribution is
    -- non-trivial. Lands on 2026-05-04, the ISO Monday of the documented 2026-05-08 launch.
    -- NULL if Meta never ran in the selected country, which correctly suppresses the
    -- stacked lines entirely.
    SELECT MIN(week) AS w FROM (
        SELECT week, v FROM meta_cy WHERE 'Total Paid DAU' != 'Attributed New Profiles'
        UNION ALL
        SELECT week, v FROM meta_np WHERE 'Total Paid DAU' = 'Attributed New Profiles')
    WHERE v > 0.5
),
frame AS (
    SELECT
        u.week,
        u.v AS uac_v,
        u.is_actual AS uac_actual_wk,
        b.v AS both_v,
        b.is_actual AS both_actual_wk,
        ms.w IS NOT NULL AND u.week >= ms.w AS has_meta
    FROM uac AS u
    LEFT JOIN both AS b USING (week)
    CROSS JOIN meta_start AS ms
),
edges AS (
    -- Each line's handoff week: its last actual week. Emitting it on BOTH the actual and
    -- the forecast series is what closes the visual gap at the join.
    SELECT
        MAX(IF(uac_actual_wk, week, NULL)) AS uac_edge,
        MAX(IF(has_meta AND both_actual_wk, week, NULL)) AS meta_edge
    FROM frame
)
SELECT
    f.week AS date,
    ROUND(IF(f.week <= e.uac_edge, f.uac_v, NULL)) AS uac_actual,
    ROUND(IF(f.week >= e.uac_edge, f.uac_v, NULL)) AS uac_forecast,
    -- Cumulative UAC + Meta (hence the uac_meta_ prefix); stacks on the UAC-only lines.
    -- NOT uac_v + meta_v: that would add two quantiles. `both_v` was aggregated inside
    -- the draw, which is the whole reason the *_totals / *_by_country tables exist.
    IF(f.has_meta AND f.week <= e.meta_edge, ROUND(f.both_v), NULL) AS uac_meta_actual,
    IF(f.has_meta AND f.week >= e.meta_edge, ROUND(f.both_v), NULL) AS uac_meta_forecast
FROM frame AS f
CROSS JOIN edges AS e
ORDER BY date
