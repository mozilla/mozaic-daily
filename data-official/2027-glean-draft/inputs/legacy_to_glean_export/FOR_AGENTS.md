# exports/ — note for an agent consuming these files

**What this is.** Firefox Desktop DAU and MAU, 2018-01-01..2026-09-08, daily, restated the way Glean telemetry counts
people ("Glean-equivalent"). Legacy telemetry is the input; Glean only exists from 2025-05-22, so before that the
Glean-equivalent series is a model translation of Legacy, and on the overlap it can be checked against published Glean.
Produced by the legacy-to-glean project (owner Brendan Wells); record is BigQuery `brwells.l2g_v4_glean_equiv_dau_v1`
(DAU) and `brwells.l2g_mau_v1_glean_equiv_mau` (MAU), state as of 2026-10-08, decisions through D-117.

**Files.** Four files, two per metric, CSV and parquet with identical rows (CSV at three decimals). Use parquet unless
you need text. Nothing else in this folder is committed.

| file | metric |
|---|---|
| `glean_equivalent_dau_history_rollups.{csv,parquet}` | DAU |
| `glean_equivalent_mau_history_rollups.{csv,parquet}` | MAU, rolling 28-day |

**Shape.** 15,865 rows per file = 5 series × 3,173 days. One row per `submission_date` × `cell`. `cell` is one of
`global_all`, `global_ex_linux`, `win_total`, `mac_total`, `linux_total`. Roll-ups only; no country detail.

**The column you want is `glean_equivalent`.** It is the headline series = `translated_from_legacy` +
`glean_only_steady`. For MAU it is *net of* `duplicate_id_inflation`, which published Glean MAU still carries, so on the
overlap compare published Glean MAU with `published_reconstruction`, not with `glean_equivalent`.

Other columns:

- `legacy_dau` / `legacy_mau` — the published Legacy input. Ratio `glean_equivalent / legacy_*` is the translation factor.
- `translated_from_legacy` — Legacy's people counted Glean's way. `glean_only_steady` — clients Glean sees that Legacy never did, carried as a constant share. `glean_only_temporary` — surges inside recorded windows; zero throughout these history files. `glean_equivalent_plus_temporary` = headline + temporary.
- MAU only: `duplicate_id_inflation` (zero here), `published_reconstruction` = headline + temporary + inflation.
- `published_glean_dau` / `published_glean_mau` — published Glean, populated 2025-05-22 on, NaN before.
- Bands, half-widths in **percent of the series**: DAU `band_correlated_pct_28d`, `band_independent_pct_28d` (apply to 28-day means, not daily values); MAU `band_correlated_pct`, `band_independent_pct` (daily). Correlated assumes cell errors add; independent assumes they average out. Use correlated for a conservative interval.
- `factor_source` is `rollup` and `pool_a_fail` is False on every row here; they matter only in the per-cell export.

**Caveats to carry forward.**

- Known data episodes are *not* removed: Legacy outage Windows 2025-08-16..11-06 and Mac 2025-08-28..12-10; Linux Glean-only excursions from 2026-03; FR/CN Glean-only windows. Legacy input dips there, so the translated series dips too.
- Model is per OS × country cell with pooled factors for small cells; roll-ups are sums of cells. Global ex-Linux is the better-behaved aggregate; Linux is the noisiest OS.
- Dates are submission dates, not activity dates.
- Reproduce or check: `python3 tooling/export_history_csv.py` then `python3 tooling/verify_exports.py` (exit 1 on mismatch). Needs the gitignored parquet copies of the delivered tables under `v4/results/` and `mau/results/`.
- Names follow `docs/glossary.md` (D-116). Older project documents use retired names: `total`/`total_ab` for glean_equivalent, `total_a`, `b_base`, `b_excess`, `b_oldbuild`, `total_with_excess`, `total_with_oldbuild`.
