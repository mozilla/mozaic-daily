-- Snapshot the pre-built Fenix paid/organic mirror, bucketed to production's country list.
--
-- The mirror carries the `growth_source` dimension the canonical aggregate cannot host, and is
-- already reconciled to canonical Fenix DAU at its trailing edge (per-day diff 0 -- verified in
-- research/mobile-organic/_index.md). It expires 2027-04-01, which is why the pipeline consumes
-- a pinned parquet rather than reading this table at run time.
--
-- `is_release` is summed away: this is the all-channel definition. Beta and nightly are never
-- marketed, so labelling them organic is correct, and all-channel preserves the published KPI
-- level.
--
-- The country bucketing MUST match `QuerySpec.build_query` exactly, or the per-country share
-- and the per-country level are computed over different populations. Both use
-- `IF(country IN (<top_DAU_markets>), country, 'ROW')`. NULL country falls to 'ROW' on the
-- production side and arrives here as '??' (the mirror build's IFNULL), which also falls to
-- 'ROW' -- so the two agree.
SELECT
  submission_date,
  IF(country IN ({countries}), country, 'ROW') AS country,
  growth_source,
  SUM(dau) AS dau
FROM `{mirror_table}`
WHERE submission_date <= DATE '{end}'
GROUP BY 1, 2, 3
