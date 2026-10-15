-- Duplicate-id inflation of real Glean desktop MAU, measured from real Glean alone (for the SECONDARY duplicate-id-corrected
-- comparison; the headline real-Glean forecast stays uncorrected, Brendan 2026-10-14): among MAU clients, distinct client ids
-- minus distinct profile_group_id (the identifier that survives client-id regeneration), by OS and version bucket (pre-148
-- builds regenerated ids; fixed in Firefox 148). Snapshot days to check the field is populated on old builds and to size the
-- term before building the daily curve. 10% sample (sample_id 0-9), counts scaled x10.
WITH snap AS (
  SELECT
    submission_date,
    CASE os_grouped WHEN 'Windows' THEN 'windows' WHEN 'Mac' THEN 'mac' WHEN 'Linux' THEN 'linux' END AS os_bucket,
    IF(app_version_major < 148, 'pre148', '148plus') AS version_bucket,
    client_id,
    profile_group_id
  FROM `moz-fx-data-shared-prod.firefox_desktop.baseline_active_users`
  WHERE submission_date IN ('2025-06-15', '2025-10-15', '2025-12-15', '2026-01-15', '2026-02-15', '2026-04-15', '2026-07-15', '2026-10-09')
    AND sample_id BETWEEN 0 AND 9
    AND app_name = 'Firefox Desktop'
    AND os_grouped IN ('Windows', 'Mac', 'Linux')
    AND is_mau
)
SELECT
  submission_date, os_bucket, version_bucket,
  10 * COUNT(DISTINCT client_id) AS mau_clients,
  10 * COUNT(DISTINCT profile_group_id) AS mau_profile_groups,
  10 * COUNTIF(profile_group_id IS NULL) AS mau_null_profile_group,
  10 * (COUNT(DISTINCT client_id) - COUNT(DISTINCT COALESCE(profile_group_id, client_id))) AS duplicate_ids
FROM snap
GROUP BY ALL
ORDER BY 1, 2, 3
