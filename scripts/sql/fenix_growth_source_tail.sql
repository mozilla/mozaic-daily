-- Extend the Fenix paid/organic mirror past its build date, to the cycle's training_end_date.
--
-- Replicates the mirror build's own SELECT so the tail is definitionally identical to the
-- snapshot it appends to. Two details carry the reconciliation guarantee and MUST NOT drift:
--
--   1. `app_name = 'Fenix'` -- this is how the canonical KPI excludes the MozillaOnline China-JV
--      build and BrowserStack test traffic. The active_users view *relabels* those to
--      'Fenix MozillaOnline' / 'Fenix browserstack' rather than dropping them, so a hand-rolled
--      distribution_id filter would miss BrowserStack and run ~0.6% high. Confirmed against the
--      production table: both are separate app_name values there too, so production's
--      `app_name IN (...)` filter already drops them.
--   2. Paid is defined only on release + Play Store + gclid. Everything else -- beta, nightly,
--      sideload, unclassified, and profiles predating new_profile_clients -- falls to organic by
--      residual. That is the authoritative marketing definition (Redash 118471), and it is what
--      makes `organic + paid_rolling_12mo + paid_prior_1yr = total` hold exactly.
--
-- Deliberately overlaps the snapshot by a few days so the join can be checked before the two
-- halves are concatenated.
WITH paid_clients AS (
  SELECT client_id, MIN(first_seen_date) AS first_seen_date
  FROM `{npc_table}`
  WHERE paid_vs_organic_gclid = 'Paid'
    AND normalized_channel = 'release'
    AND install_source = 'com.android.vending'
    AND first_seen_date <= DATE '{end}'
  GROUP BY client_id
),

au AS (
  SELECT
    submission_date,
    IFNULL(country, '??') AS country,
    client_id
  FROM `{au_table}`
  WHERE is_dau
    AND app_name = 'Fenix'
    AND submission_date BETWEEN DATE '{start}' AND DATE '{end}'
)

SELECT
  au.submission_date,
  IF(au.country IN ({countries}), au.country, 'ROW') AS country,
  CASE
    WHEN pc.client_id IS NULL THEN 'organic'
    WHEN pc.first_seen_date >= DATE_SUB(au.submission_date, INTERVAL 365 DAY)
      THEN 'paid_rolling_12mo'
    ELSE 'paid_prior_1yr'
  END AS growth_source,
  COUNT(*) AS dau
FROM au
LEFT JOIN paid_clients AS pc
  USING (client_id)
GROUP BY 1, 2, 3
