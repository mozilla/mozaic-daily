# `data-official/2026-06/` — June 2026 forecast cycle

> **Retention:** June is the **N-1 reference cycle** — its final deliverables + canonical parquets
> stay on disk as the prior-cycle comparison baseline. Large intermediates (fitted `.pkl`, scratch
> parquets, `.zip` bundles) are archived to `gs://…/june-2026/` and removed from the pruned working
> branch (`clean-slate`); they remain in the `july-forecast` branch. See "Present vs Archived" at the bottom.

Forecast_start_date evolved across the cycle: **2026-05-13** initial → **2026-05-17** (cap-426 fix) → **2026-05-21** (param-scan rerun) → **2026-05-26** (canonical "best for now" handoff). Applies `h` (headwinds) on desktop+mobile and `m` (marketing-lift) on mobile, producing `.adj-hm.` composites.

## Canonical handoff (most recent — use this)

- **`june_canonical_v2026-05-27.ipynb`** — canonical producer notebook for the 2026-05-26 forecast. Sets up paths, loads June + April parquets, queries BQ actuals through `CURRENT_DATE("America/Los_Angeles") - 2`, applies the June headwind, plots desktop + mobile (with and without April), prints Dec-15 28dMA tables (incl. slack-friendly single + side-by-side variants).
- **`export_canonical_curves.py`** — headless reproducer for the CSV exports under `csv/`. Default mode writes the ALL-level `csv/june_canonical_curves.csv`; `--per-country` writes one raw (no-headwind) CSV per forecasted country into `csv/per_country/`. Run with `source .venv/bin/activate && python3 data-official/2026-06/export_canonical_curves.py [--per-country]`. Same logic as the notebook; no plots. Forecast 28dMA curves are smoothed across the actuals→forecast seam via `display_ma` (variance-matched transition: the first 27 forecast days are rebuilt to carry the recent actuals' weekly amplitude so the transition rides the forecast's true trend; spliced to the clean forecast-only MA at +27d, so far-horizon incl. Dec-15 is byte-identical) — see `research/ma-seam-turbulence/` for the diagnosis, the April-backtest gate (`backtest_seam.py`), and `tests/test_export_canonical_curves.py` for the regression lock.
- **`csv/`** — public-facing CSV exports for downstream consumers. Holds `june_canonical_curves.csv` (365 rows × 13 columns of 28dMA DAU — June + April forecasts, BQ actuals, with/without Iran, headwind applied) and `per_country/june_canonical_curves.<CC>.no-headwinds.csv` (15 files, 7 columns each, raw model output). `csv/README.md` is the single data card covering both, with the column dictionary and an AI-agent plotting prompt.

## Earlier producer notebooks (kept for history)

| File | Output |
|---|---|
| `june_composite_forecast.ipynb` | Headwind-only composite: `june_composite_forecast_28ma.adj-h.csv` |
| `june_composite_forecast_no_headwinds.ipynb` | Counterfactual for sensitivity discussions |

The intermediate composite CSVs (`june_composite_forecast_28ma.adj-h.csv`, `june_thresh_aligned_composite_forecast_28ma.adj-hm.csv`, `june_with_marketing_composite_forecast_28ma.adj-hm.csv`, `june_mobile_plot_series.adj-h.csv`) come from earlier producer-notebook runs and reflect each milestone's forecast-start + adjustment state. See sidecar `.meta.json` for provenance; the canonical CSV above is the one to share.

## Diagnostics

| File | What it does |
|---|---|
| `june_vs_april_desktop_diagnostic.ipynb` | Why June desktop forecast levels lower than April — trend + changepoint analysis |
| `june_mobile_dau_forecast_comparison.ipynb` | Compares June mobile DAU across region/OS splits |

## Per-config subdirs

| Dir | Config |
|---|---|
| `desktop_cps0.15983_thresh50_recent13_clip0.6/` | Initial June config (threshold=-0.50, pre-cap-426) |
| `desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/` | **Canonical desktop**: threshold=-0.05 matching April, cap-426 fix. Holds 5-17, 5-21, and **5-26** raw + plus_iran parquets + sidecars |
| `desktop_cps0.15983_thresh032_recent13_clip0.6_cap426/` | Default-threshold cap-426 variant for comparison |
| `mobile_cps0.02_thresh32_recent13_clip0.6/` | Initial June mobile |
| `mobile_cps0.02_thresh32_recent13_clip0.6_cap426/` | **Canonical mobile**: cap-426 fix; `.adj-m.` parquets carry marketing-lift. Holds 5-17 and **5-26** adj-m + plus_iran parquets + sidecars |

## Adjustments

- `adjustments/headwind.json` — June's headwind ramp spec. Desktop anchor: **−1,420,000** (April's was −1,497,870 — June reflects ~5% Prophet absorption of the Win10 headwind from Apr–May actuals; sized to match April's Dec-15 28dMA within ±5k). Mobile anchor: −27,162 (unchanged).
- `marketing/` — Marketing-lift `m` adjustment artifacts:
  - `marketing.json` — spec consumed by the pipeline (`applies_to_forecast_start: 2026-05-26`, points at the v2 hybrid parquet)
  - `marketing_lift_model.real_data_v2.hybrid.<date>.parquet` — daily lift series (current canonical: `2026-05-22.parquet`; v2 model has a peak-then-decline shape that puts year-end magnitude materially below v1's monotonic projection)
  - Older v1 + intermediate-hybrid parquets retained for history; `marketing.json`'s `data_file` is the source of truth

## Other artifacts

- `stakeholder_scenarios/` + `stakeholder_scenarios_2026-06.zip` — pre-packaged scenario CSVs for leadership review. `stakeholder_targets.json` defines the Dec-15 mobile/desktop low/baseline/stretch values used as marker points in the canonical notebook plots.
- `parameters.json` — top-level config snapshot.
