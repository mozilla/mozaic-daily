# Stale references flagged at the August 2026 button-down (2026-09-04)

Produced by Phase 3 of `.claude/skills/cycle-button-down/`. **Report only — nothing here was edited.**
Each row is a code file (py/ipynb/sh) that hardcodes a path whose contents left the working tree when
August was archived to `gs://moz-data-science-brwells-bucket/mozaic-daily-archive/august-2026/`.
Narrative `.md` mentions are not listed; they describe history and are correct as written.

Pull a blob back with `gcloud storage cp -r gs://…/august-2026/<mirror path> <local path>` if a script
needs it, or repoint the script at the September build.

## Cycle-scoped constants to repoint for September

| script | constant | current value |
|---|---|---|
| `scripts/mobile_scoring.py` | `FORECAST_START` / `TARGET_DEC15` | `2026-07-28` / `17_923_869` (July's Dec-15) |
| `scripts/mobile_scoring.py`, `scripts/score_near_horizon.py` | `DEFAULT_HEADWIND` | `data-official/2026-08/adjustments/headwind.json` |
| `scripts/score_near_horizon.py` | `DEFAULT_TARGET_DATE` | `2026-08-25` |
| `scripts/export_desktop_no_headwind_csv.py`, `scripts/export_desktop_ex_ir_cn_csv.py` | `CSV_DIR`, `CURRENT_/PRIOR_ADJUSTMENTS_DIR`, `FORECAST_START`, `PREV_FORECAST_START` | 2026-08 / 2026-07, `2026-08-02` / `2026-07-06` |
| `scripts/run_mobile_gradient.py` | `FORECAST_START`, `RAW_CACHE_DIR`, `DEFAULT_RESULTS_DIR` | `2026-07-28`, `mobile_uac_meta_2026-07-28` (**archived**), `mobile-aug/results` |
| `scripts/run_aug_trough_gradient.py` | `RESULTS_ROOT`, `FORECAST_START` | `aug22-retune`, `2026-07-06` (July-era, never repointed for August) |
| `scripts/mobile_app_breakdown.py` | default build dir | `mobile_baseline_2026-07-28` (**archived**) |
| `scripts/fetch_raw_pull.py` | default output dir | `mobile_rawpull_2026-08-02` (**archived**) |
| `scripts/build_fenix_organic_split.py` | `--production-raw` example path | `mobile_uac_meta_2026-07-28/...` (**archived**) |
| `scripts/tile_corr_distribution.py`, `scripts/run_s01_gradient.py`, `run_summer_trough_grid.py`, `run_trend_only_grid.py` | default pkl / raw-cache path | `desktop_baseline_2026-07-28/...` (**archived**) |
| `scripts/verify_training_rows_are_actuals.py` | example paths | `desktop_locked/`, `mobile_baseline_2026-07-28/` (**archived**) |
| `tests/test_mobile_scoring.py:201` | fixture path | `mobile_organic_2026-07-28` — check whether the test reads the parquet (archived) or only the sidecar |

## Code files referencing archived or deleted paths

| referenced path | status after prune | files | file (lines) |
|---|---|--:|---|
| `2026-08/desktop_locked` | blobs archived (parquet/pkl/raw pull); sidecars + README stay | 11 | `data-official/2026-08/_backup_mobile_methodology_2026-07-31/august_canonical_v2026-07-28.ipynb` (272)<br>`data-official/2026-08/seam_fix_before_after.ipynb` (139)<br>`research/autumn-decoupling/curves.py` (82)<br>`research/ma-seam-turbulence/eval_deseason_variant.py` (53)<br>`research/ma-seam-turbulence/eval_recon_edge_fix.py` (62)<br>`research/ma-seam-turbulence/plan_probe_fix_a.py` (207)<br>`research/ma-seam-turbulence/seam_step_diagnosis.ipynb` (119)<br>`research/param-scans/aug25-gap/plot_candidates.py` (7, 57)<br>`research/param-scans/aug25-gap/run_gradient_round1.sh` (7)<br>`research/param-scans/aug25-gap/score_gradient.py` (3, 44)<br>`scripts/verify_training_rows_are_actuals.py` (45) |
| `desktop_baseline_2026-07-28` | blobs archived; sidecars + README stay. Was the shared raw-pull cache every desktop scan symlinked | 16 | `data-official/2026-08/desktop_adjustment_ladder.ipynb` (34, 126, 1014)<br>`research/autumn-decoupling/curves.py` (64)<br>`research/ma-seam-turbulence/diagnose_recon_edge_bias.py` (23)<br>`research/ma-seam-turbulence/eval_recon_edge_fix.py` (64)<br>`research/param-scans/aug25-gap/run_blend_round2.py` (37)<br>`research/param-scans/aug25-gap/run_corr_round3.py` (37)<br>`research/param-scans/aug25-gap/run_gradient_round1.sh` (23)<br>`research/param-scans/aug25-gap/score_blend.py` (40)<br>`research/param-scans/summer-trough-v2/build_grid_report.py` (61)<br>`research/param-scans/summer-trough-v2/build_trend_only_report.py` (54)<br>`research/param-scans/summer-trough-v2/s01_canonical_desktop.ipynb` (146)<br>`scripts/run_s01_gradient.py` (52)<br>`scripts/run_summer_trough_grid.py` (64)<br>`scripts/run_trend_only_grid.py` (69)<br>`scripts/score_near_horizon.py` (24)<br>`scripts/tile_corr_distribution.py` (24) |
| `desktop_candidate_aug25` | blobs archived; sidecars stay | 1 | `research/param-scans/aug25-gap/plot_final.py` (31) |
| `desktop_s01_REVERT_2026-07-29` | blobs archived; REVERT.md + sidecars stay (revert window closed) | 2 | `data-official/2026-08/_backup_mobile_methodology_2026-07-31/august_canonical_v2026-07-28.ipynb` (248, 522, 1163)<br>`data-official/2026-08/august_canonical_v2026-07-28.ipynb` (316, 665, 1312, 2860) |
| `mobile_cpr0725_2026-07-28` | blobs archived; sidecars stay | 1 | `research/param-scans/mobile-aug/tailwind_exercise.py` (40) |
| `mobile_organic_2026-07-28` | blobs archived; sidecars stay | 6 | `research/mobile-organic/build_paid_seam_notebook.py` (76)<br>`research/mobile-organic/paid_seam_methods.ipynb` (114)<br>`research/mobile-organic/reproduce_prototype.py` (97)<br>`scripts/mobile_scoring.py` (48)<br>`scripts/run_mobile_gradient.py` (9)<br>`tests/test_mobile_scoring.py` (201) |
| `mobile_uac_meta_2026-07-28` | blobs archived (incl. the raw pull the mobile scans used); sidecars stay | 3 | `data-official/2026-08/marketing/experiment_july_methodology/experiment_july_methodology.ipynb` (84, 124)<br>`scripts/build_fenix_organic_split.py` (19)<br>`scripts/run_mobile_gradient.py` (90) |
| `mobile_baseline_2026-07-28` | blobs archived; sidecars + README stay | 8 | `data-official/2026-08/_backup_mobile_methodology_2026-07-31/august_canonical_v2026-07-28.ipynb` (276)<br>`data-official/2026-08/marketing/august_marketing_lift.ipynb` (73, 99, 795, 835, 915)<br>`data-official/2026-08/seam_fix_before_after.ipynb` (144)<br>`research/ma-seam-turbulence/check_delivered_numbers.py` (109)<br>`research/ma-seam-turbulence/eval_deseason_variant.py` (57)<br>`research/ma-seam-turbulence/plan_probe_fix_a.py` (211)<br>`scripts/mobile_app_breakdown.py` (63)<br>`scripts/verify_training_rows_are_actuals.py` (57) |
| `mobile_adjm_REVERT_2026-07-31` | blobs archived; REVERT.md + sidecars stay | 1 | `data-official/2026-08/august_canonical_v2026-07-28.ipynb` (69, 349) |
| `mobile_rawpull_2026-08-02` | raw pull archived; _index.md stays | 1 | `scripts/fetch_raw_pull.py` (19) |
| `2026-08/adjustment_isolation` | run blobs archived; sidecars + _index.md stay | 2 | `data-official/2026-08/desktop_adjustment_ladder.ipynb` (121, 1021)<br>`research/ma-seam-turbulence/backtest_recon_variants.py` (52) |
| `_backup_mobile_methodology_2026-07-31` | blobs archived; RESTORE.md + MANIFEST stay | 1 | `data-official/2026-08/august_canonical_v2026-07-28.ipynb` (351) |
| `marketing/experiment_july_methodology/forecast` | pkl + parquet archived; sidecars stay | 1 | `data-official/2026-08/marketing/experiment_july_methodology/experiment_july_methodology.ipynb` (85) |
| `summer-trough-v2/{grid,s01_gradient,trend_only,phase1}` | probe pkls/parquets archived; parameters.json + .meta.json stay | 9 | `data-official/2026-08/_backup_mobile_methodology_2026-07-31/august_canonical_v2026-07-28.ipynb` (21, 1147, 1978, 2040)<br>`data-official/2026-08/august_canonical_v2026-07-28.ipynb` (88, 1296, 2835, 2900)<br>`research/autumn-decoupling/attribute_autumn.py` (18)<br>`research/autumn-decoupling/curves.py` (75)<br>`research/ma-seam-turbulence/diagnose_recon_edge_bias.py` (26)<br>`research/param-scans/summer-trough-v2/s01_canonical_desktop.ipynb` (140)<br>`scripts/run_s01_gradient.py` (54)<br>`scripts/run_summer_trough_grid.py` (66)<br>`scripts/run_trend_only_grid.py` (71) |
| `mobile-aug/results` | probe pkls/parquets archived; sidecars stay | 1 | `scripts/run_mobile_gradient.py` (92) |
| `aug25-gap/runs` | probe parquets archived; sidecars stay | 1 | `research/param-scans/aug25-gap/run_gradient_round1.sh` (21) |
| `headwinds/aug-post-seam-retune/refit_2026-08-17` | refit pkl archived | 1 | `research/headwinds/aug-post-seam-retune/headwind_retune_experiment.ipynb` (996) |
| `data-official/iran_synthetic` | DELETED (tracked; copy in april-2026/ + branch history) | 7 | `data-official/2026-06/june_composite_forecast.ipynb` (796, 797, 799, 807, 808)<br>`data-official/2026-06/june_composite_forecast_no_headwinds.ipynb` (752, 753, 755, 763, 764)<br>`scripts/add_iran_to_forecast.py` (6, 51)<br>`scripts/generate_iran_fill.py` (11)<br>`scripts/generate_iran_synthetic.py` (17, 18, 21, 59, 60)<br>`scripts/run_comparison_forecasts.py` (9, 10, 11, 15, 16, 45, …)<br>`src/mozaic_daily/data.py` (19) |

## Standing dependencies (state every cycle)

- `_archive/` and `research/ma-seam-turbulence/` import the frozen `data-official/2026-06/export_canonical_curves.py`. **2026-06 is retained this cycle** (3-month window), so nothing breaks; when June leaves the window at the October roll-forward this must be resolved first.
- `data-official/2026-06/marketing/marketing_lift_model.real_data_v2.hybrid.2026-05-22.parquet` and `mozillaonline/june_delivered_mo_tailwind.json` are read by July-cycle code. Same retention note.
- `data-official/2026-08/marketing/marketing_lift_model.total.2026-06-29.parquet` is what `p` reads as the paid level. It is tracked (gitignore exception) and stays.
