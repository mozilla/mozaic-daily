-- Daily global Firefox Desktop DAU split by Windows version.
--
-- Source: the desktop-only derived table that backs
-- `telemetry.active_users_aggregates` (whose mobile UNION defeats partition
-- pruning). `os_version_build` here is exposed as `os_version` by that view,
-- which is the column the production forecast segments on. Scope matches
-- production exactly: app_name = "Firefox Desktop", all countries.
--
-- Buckets mirror the forecast's segment definitions (queries.py:259) so the
-- BigQuery split is reconcilable against the forecast parquet:
--   modern_windows = LIKE '%windows 1%'          -> win10 + win11 + win1x_other
--   winX           = windows AND NOT 10 AND NOT 11 -> winX_supported + winX_other
-- The forecast cannot separate win10 from win11 (both collapse into
-- modern_windows), which is why this split has to come from BigQuery.
--
-- Date design: daily for 2026-01-01..2026-07-27 (the window the anchor's
-- implied loss must show up in), weekly Wednesdays for 2025 (pre/post-EOL
-- shape and the winX decay control). Weekly sampling on a fixed weekday means
-- day-of-week effects cancel without needing a 28d MA.
SELECT
  submission_date,
  CASE
    WHEN os_version_build = 'Windows 10' THEN 'win10'
    WHEN os_version_build = 'Windows 11' THEN 'win11'
    WHEN LOWER(os_version_build) LIKE '%windows 1%' THEN 'win1x_other'
    WHEN os_version_build IN ('Windows 7', 'Windows 8', 'Windows 8.1') THEN 'winX_supported'
    WHEN LOWER(os_version_build) LIKE '%windows%' THEN 'winX_other'
    ELSE 'non_windows'
  END AS os_bucket,
  SUM(dau) AS dau
FROM `moz-fx-data-shared-prod.firefox_desktop_derived.active_users_aggregates_v4`
WHERE app_name = 'Firefox Desktop'
  AND (
    submission_date BETWEEN '2026-01-01' AND '2026-07-27'
    OR submission_date IN (
    '2025-01-01', '2025-01-08', '2025-01-15', '2025-01-22', '2025-01-29', '2025-02-05',
    '2025-02-12', '2025-02-19', '2025-02-26', '2025-03-05', '2025-03-12', '2025-03-19',
    '2025-03-26', '2025-04-02', '2025-04-09', '2025-04-16', '2025-04-23', '2025-04-30',
    '2025-05-07', '2025-05-14', '2025-05-21', '2025-05-28', '2025-06-04', '2025-06-11',
    '2025-06-18', '2025-06-25', '2025-07-02', '2025-07-09', '2025-07-16', '2025-07-23',
    '2025-07-30', '2025-08-06', '2025-08-13', '2025-08-20', '2025-08-27', '2025-09-03',
    '2025-09-10', '2025-09-17', '2025-09-24', '2025-10-01', '2025-10-08', '2025-10-15',
    '2025-10-22', '2025-10-29', '2025-11-05', '2025-11-12', '2025-11-19', '2025-11-26',
    '2025-12-03', '2025-12-10', '2025-12-17', '2025-12-24', '2025-12-31'
    )
  )
GROUP BY submission_date, os_bucket
ORDER BY submission_date, os_bucket
