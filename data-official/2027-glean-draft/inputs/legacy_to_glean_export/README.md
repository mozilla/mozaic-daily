# exports/

Exports of the delivered Glean-equivalent histories, 2018-01-01 .. 2026-09-08, daily, produced by
`tooling/export_history_csv.py` from the delivered tables (`brwells.l2g_v4_glean_equiv_dau_v1`,
`brwells.l2g_mau_v1_glean_equiv_mau`; decisions through D-117, tables last re-uploaded 2026-10-08).
Check them with `tooling/verify_exports.py` (exit 1 on any mismatch against the delivered copies).

| file | rows | committed |
|---|---|---|
| `glean_equivalent_dau_history_rollups.csv` / `.parquet` | global_all, global_ex_linux, win_total, mac_total, linux_total (15,865 rows) | yes |
| `glean_equivalent_mau_history_rollups.csv` / `.parquet` | same for MAU | yes |
| `glean_equivalent_dau_history_by_cell.csv` | every cell and roll-up (733 keys) | no (≈ 190 MB, gitignored; `--by-cell`) |
| `glean_equivalent_mau_history_by_cell.csv` | same for MAU | no (≈ 240 MB) |

The roll-up files are the export of record; CSV and parquet hold the same rows (CSV at three decimals).

Columns (names per `docs/glossary.md`, D-116):
- `submission_date`, `cell` (OS × country key or roll-up), `factor_source` (own cell or pool the factors come from; `rollup` on the roll-up rows), `pool_a_fail` (flag: pooled cell failing its bar).
- `legacy_dau` / `legacy_mau`: the published Legacy input.
- `glean_equivalent`: the headline, translated_from_legacy + glean_only_steady. For MAU it is net of the duplicate-id inflation still present in published Glean.
- `translated_from_legacy`, `glean_only_steady`, `glean_only_temporary` (zero in history), `glean_equivalent_plus_temporary`.
- MAU only: `duplicate_id_inflation` (zero in history), `published_reconstruction` = glean_equivalent + glean_only_temporary + duplicate_id_inflation, the series that matches what Glean publishes today.
- `published_glean_dau` / `published_glean_mau`: published Glean on the overlap (2025-05-22 on), empty before.
- Bands: half-widths in % of the series; DAU on 28-day means (`band_correlated_pct_28d`, `band_independent_pct_28d`), MAU daily (`band_correlated_pct`, `band_independent_pct`). Correlated = cell errors add; independent = they average out.

## Forecast cut: `glean_equivalent_{dau,mau}_history_cells_fc15.{parquet,csv}` (2026-10-12)

Cut by `tooling/export_cells_fc15.py` for the mozaic-daily 2027 DAU/MAU forecast, from the same delivered table
copies (sha1 DAU `62dab5e22be8744cea51aae120520e13d63a73ff`, MAU `2f6f779c845e254f575a53bbf74afda6396852a5`).
168,169 rows per metric = 53 series × 3,173 days, 2018-01-01..2026-09-08.

- **Cells:** 15 countries × 3 OS as `win_XX`, `mac_XX`, `linux_XX` for US, BR, CA, MX, AR, IN, ID, JP, CN, DE, FR, PL, RU, IT, IR.
- **ROW:** `win_ROW`, `mac_ROW`, `linux_ROW` = the OS roll-up minus the 15 country cells on every additive column
  (Legacy input, all series components, published Glean), so cells + ROW = `{os}_total` exactly; the script exits 1
  if the identity fails by more than 0.001. ROW bands are NaN (the model defines bands per cell, not as differences);
  `factor_source` = `rollup`, `pool_a_fail` False. Published Glean on ROW is NaN before 2025-05-22 like everywhere else.
- **Roll-ups:** the five roll-up rows are included so the identity can be re-checked by the consumer.
- **Columns:** the roll-up export's columns plus the absolute-unit bands from the delivered tables: DAU
  `band_corr_28d_dau`, `band_indep_28d_dau`, `band_drift`, `band_a4`; MAU `band_corr_mau`, `band_indep_mau`,
  `band_drift`, `band_spread`, `band_test` (people, not percent).
- **Per-cell flags:** `fc15_cells_summary.csv` lists `factor_source`, `pool_a_fail` and mean 2026 Glean-equivalent per
  cell and metric. Pooled cells among the 15 (factor_source `*_mid` / `*_small`): most Linux cells, Mac × AR/BR/CN/ID/IN/IT/PL/RU/IR,
  Windows × AR/CN/FR/ID/IR/JP/MX/PL/RU on DAU (MAU: Windows × CN/FR on pool + own f,u,r; IR/JP/RU on pool). Iran is
  small on Mac and Linux (`*_small` pool).

Verification output of the cut run: `dau: 168,169 rows, 53 series, sum identity holds within 0.001`;
`mau: 168,169 rows, 53 series, sum identity holds within 0.001`.

Two companion tables for the forecast's splice decision (2026-10-12, computed from the same delivered copies):
- `fc15_overlap_gap_by_month.csv`: per metric, roll-up and month over the overlap, the monthly means of the
  components and of published Glean, `headline_minus_published_pct`, the temporary and duplicate-id terms as % of
  published, and `unexplained_pct` = headline − published + temporary + duplicate-id (what no delivered term accounts for).
- `fc15_seam_step_2026-08-23.csv`: per metric, roll-up and fc15 cell, headline ÷ published − 1 over the 14 days before
  the Fx 154 rollout (2026-08-02..15) and the 14 days after the plateau (2026-08-26..09-08), the same for the
  published reconstruction, and the difference in pp (`seam_step_*_pp`).

Exclusion windows (Legacy outage Oct–Nov 2025 per OS, Linux and FR/CN Glean-only windows) are not removed from the series; see `v4/results/windows.csv`, `windows_members.csv`, `mau/results/windows_frcn.csv`.
