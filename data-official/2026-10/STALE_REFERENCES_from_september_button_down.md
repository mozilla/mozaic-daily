# Stale references left by the September 2026 button-down (2026-10-05)

Produced by Phase 3 of `/cycle-button-down` closing `2026-09`. Phase 3 edits nothing; this file is the
repointing to-do list for the October monthly update. Retention window now in force: `2026-07`,
`2026-08`, `2026-09` on disk; `2026-06` **deferred again** (third time, Brendan's decision at GATE 0)
because it is still read by live code — see "Standing dependencies".

**What actually left the disk this time** is narrow: every `mozaic_objects.*.pkl` under
`data-official/2026-09/` (43) and `data-official/2026-08/` (7, re-pulled or newly made after the August
button-down), the research pickle `research/forecast-vs-summer-actuals/data/pkl/`, `tmp/regression/`
(two regression-rerun pickles, deleted without archiving — they reproduce archived builds) and
`.DS_Store` files. **Every forecast and raw-pull parquet, sidecar, spec, CSV, notebook and plot of both
cycles is still on disk**, so a path reference below is "fine until repointed", not broken, unless it
names a pickle.

## 1. Cycle-scoped script constants (`scripts/`)

**Roll-forward edits (Phase 5 step 5, 2026-10-05):** the unambiguous ones were changed — `DEFAULT_HEADWIND` in
`mobile_scoring.py` and `score_near_horizon.py` → the `2026-10/adjustments/` copies; `CSV_DIR`,
`CURRENT_ADJUSTMENTS_DIR`, `PRIOR_ADJUSTMENTS_DIR` in `export_desktop_no_headwind_csv.py` and
`export_desktop_ex_ir_cn_csv.py` → `2026-10/csv`, `2026-10/adjustments`, `2026-09/adjustments`; and
`PREV_FORECAST_START` in `export_desktop_no_headwind_csv.py` → `2026-09-09`. **Everything marked *needs human*
below was left as it was.** The table shows the values as found at the button-down.

| script | constant (line) | value now | points at | needs |
|---|---|---|---|---|
| `mobile_scoring.py` | `FORECAST_START` (75) | `2026-09-02` | September's **first** seam, never moved to 2026-09-09 | **needs human** — repoint to the October seam once chosen; `TARGET_DEC15` too |
| `mobile_scoring.py` | `DEFAULT_HEADWIND` (78) | `data-official/2026-09/adjustments/tou_mobile_headwind.json` | retained | edited → `2026-10/adjustments/tou_mobile_headwind.json` (provisional copy) |
| `export_desktop_no_headwind_csv.py` | `CSV_DIR` (57), `CURRENT_ADJUSTMENTS_DIR` (61), `PRIOR_ADJUSTMENTS_DIR` (62) | `2026-09/csv`, `2026-09/adjustments`, `2026-08/adjustments` | retained | edited → `2026-10/csv`, `2026-10/adjustments`, `2026-09/adjustments` |
| `export_desktop_no_headwind_csv.py` | `FORECAST_START` (64), `PREV_FORECAST_START` (65) | `2026-09-02`, `2026-08-02` | **`FORECAST_START` is the stale first seam** — it was never moved to 2026-09-09, so this script has not matched the published September curve since 2026-09-10 | **needs human** for `FORECAST_START`; `PREV_FORECAST_START` edited → `2026-09-09` |
| `export_desktop_ex_ir_cn_csv.py` | `CSV_DIR` (67), `CURRENT_ADJUSTMENTS_DIR` (81), `PRIOR_ADJUSTMENTS_DIR` (82) | `2026-09/…`, `2026-08/adjustments` | retained | edited → `2026-10/csv`, `2026-10/adjustments`, `2026-09/adjustments` |
| `export_desktop_ex_ir_cn_csv.py` | `FORECAST_START` (84), `PREV_FORECAST_START` (85) | `2026-08-02`, `2026-07-06` | **still August's seams** — this script was never repointed for September at all | **needs human** — both dates (left untouched so they stay mutually consistent) |
| `export_desktop_daily_csv.py` | `CSV_DIR` (67), `PLOT_PATH` (69), `CURRENT_ADJUSTMENTS_DIR` (85), `PRIOR_ADJUSTMENTS_DIR` (86) | `2026-09/…`, `2026-08/adjustments` | retained | **needs human** — not edited: `tests/test_export_desktop_daily_csv.py` reads the published September CSV through `CSV_DIR` and pins `FORECAST_START` |
| `export_desktop_daily_csv.py` | `FORECAST_START` (89), `PREV_FORECAST_START` (90) | `2026-09-09`, `2026-08-02` | correct for September | **needs human** (test-pinned) |
| `mobile_app_breakdown.py` | `DEFAULT_FORECAST` (66) | `…/mozaic_daily_forecast.2026-09-02.gm-D.adj-p.parquet` under a variable still named `_AUGUST_MOBILE` | the 09-04 mobile build, retained, **not** the canonical Med build | **needs human** — October canonical mobile parquet once it exists; rename the variable |
| `score_near_horizon.py` | `DEFAULT_TARGET_DATE` (64), `DEFAULT_HEADWIND` (66) | `2026-08-25`, `data-official/2026-09/adjustments/headwind.json` | August's trough date; retained spec | **needs human** for the target date; `DEFAULT_HEADWIND` edited → `2026-10/adjustments/headwind.json` |
| `run_aug_trough_gradient.py` | `FORECAST_START` (47) | `2026-07-06` | July's seam — an August-era scan driver | **needs human** / leave; historical driver, not re-run |

## 2. References to files that LEFT the disk (pickles)

All of these need a `gcloud storage cp` from the archive before they run again. None are in `src/` or
`tests/`.

| file (line) | pickle referenced | archive location |
|---|---|---|
| `scripts/compute_forecast_intervals.py` (13, usage docstring) | `2026-08/desktop_raw_ci_2026-08-02/<slug>/mozaic_objects.legacy_desktop.2026-08-02.pkl` | `august-2026/data-official/2026-08/desktop_raw_ci_2026-08-02/` (docstring example only) |
| `scripts/tile_corr_distribution.py` (24, usage docstring) | `2026-08/desktop_baseline_2026-07-28/<slug>/mozaic_objects.legacy_desktop.2026-07-28.pkl` | `august-2026/…/desktop_baseline_2026-07-28/` (docstring example only) |
| `research/forecast-vs-summer-actuals/seasonality.py` (90) | `2026-08/desktop_g01_2026-08-02/<slug>/mozaic_objects.legacy_desktop.2026-08-02.pkl` | `august-2026/…/desktop_g01_2026-08-02/` — **this one is code, not a docstring**; the audit re-pulled it on 2026-09-15 and it is pruned again now |
| `research/forecast-vs-summer-actuals/_index.md` (111, 114) | `data/pkl/mozaic_objects.legacy_desktop.2026-07-06.pkl`; the 2026-08-02 pickle above | `september-2026/research/forecast-vs-summer-actuals/data/pkl/`; `august-2026/…` |
| `research/forecast-intervals/september-2026-{desktop,mobile}/_index.md` (93 / 88), `*_raw_summary.json` (3), `research/forecast-intervals/september_2026_raw_intervals.ipynb` (118, 135, cell output) | `2026-09/desktop_raw_ci_2026-09-09/…/mozaic_objects.legacy_desktop.2026-09-09.pkl`, `2026-09/mobile_raw_ci_2026-09-09/…/mozaic_objects.glean_mobile.2026-09-09.pkl` | `september-2026/data-official/2026-09/{desktop,mobile}_raw_ci_2026-09-09/` |
| `research/forecast-intervals/august-2026-desktop/*` (code_snapshot docstring, summary json, notebook output) | August's raw-CI pickle (absolute path under a `mozaic-daily-august` worktree) | `august-2026/…/desktop_raw_ci_2026-08-02/` |
| `research/param-scans/summer-trough-v2/grid/FINDINGS.md` (114) | `2026-08/desktop_baseline_2026-07-28/…/mozaic_objects.legacy_desktop.2026-07-28.pkl` | `august-2026/…` (prose) |

## 3. References to retained paths that will go stale at the next prune (informational)

`data-official/2026-08/` and `data-official/2026-07/` are read, by path, from: `CLAUDE.md`,
`.claude/skills/{cycle-button-down/reference/history.md, ingest-adjustment/SKILL.md}`,
`research/autumn-decoupling/{curves.py, LOG.md}`, `research/forecast-vs-summer-actuals/{series.py,
seasonality.py, _index.md, LOG.md}`, `research/headwinds/{build_report.py, win10_anchor_validation.ipynb,
aug-post-seam-retune/*}`, `research/ma-seam-turbulence/*` (eight scripts + notebook), `research/mobile-organic/{build_paid_seam_notebook.py,
paid_seam_methods.ipynb, reproduce_prototype.py}`, `research/param-scans/{_index.md, aug25-gap/*, …}`,
`tests/test_kpi_sheet.py` (217–218, July CSVs — **a test fixture**), `tests/test_score_near_horizon.py`
(133–137, `2026-07/desktop_locked` + July headwind — **a test fixture**), `scripts/plot_forecast_set.py`
(121, default out-dir `2026-07/plots`), `scripts/generate_iran_fill.py` (12, `2026-07/iran_fill/FILL_FORMAT_SPEC.md`).
All retained through at least the November roll-forward (`2026-07` leaves then). The two test fixtures
pinned to July must be re-pinned or vendored before that.

## 4. Standing dependencies (state every cycle)

- `_archive/`, `research/ma-seam-turbulence/`, `src/mozaic_daily/seam_ma.py` (docstring) and
  `tests/test_seam_ma.py` (docstring) refer to the frozen `data-official/2026-06/export_canonical_curves.py`.
  Pruning `2026-06` breaks `_archive/tests/`.
- `data-official/2026-06/` still holds the `real_data_v2` marketing parquet and
  `june_delivered_mo_tailwind.json` that July's (retained) specs read, and `scripts/regenerate_composites.py`
  (80–113) and `scripts/run_param_scan.py` (20, usage text) name June build directories.
- **Decision 2026-10-05: `2026-06` stays on disk.** Before it can leave, the current cycle must own copies of
  `export_canonical_curves.py` (or `_archive/` must vendor it) and the June marketing parquet.
