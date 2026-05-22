# Marketing-lift model (real-data variant)

Self-contained workspace for a marketing-lift model whose lift series comes from a **marketing-team CSV** (`data/paid_dau_weekly_forecast.20260519.csv`) rather than the convolution-fit retention model in `.claude/worktrees/marketing-lift/marketing_lift_model/`.

Branch: `worktree-marketing-real-data` (off `june-forecast`, which has the `m` adjustment plumbing already landed).

## Why a new model

The current `m` adjustment ships a lift series derived from `(YoY-corrected Fenix excess NP) ⊛ (3-param retention curve) × historical scale factor`, projected forward piecewise-linearly. It extrapolates 5× from the 8-week post-launch observation (363k) to year-end (~1.78M), with a parametric retention shape and a YoY baseline as soft assumptions. The marketing team has now produced an independent paid-DAU forecast that is more grounded — that CSV is what we ingest here.

## Data source

**Canonical:** the marketing team's CSV at `data/paid_dau_weekly_forecast.20260519.csv` — weekly cadence, full-year 2026 projection. We use the `week` and `total_paid_dau` columns.

**Sanity-check reference (NOT the source):** STMO query 118452 ("Fenix DAU by Acquisition Type") from the **DS team** at https://sql.telemetry.mozilla.org/queries/118452/source. Built on `mozdata.fenix.active_users` with a cohort-based attribution definition (any client ever paid-acquired via Play Store + gclid → all their future DAU rows count as Paid). The CSV is **not** derived from this query — it is a parallel view of the same underlying signal, useful for visual magnitude cross-check.

**DO NOT USE `*_marketing_geo_testing_v1` tables.** They are a third, separate attribution definition flagged as wrong by the user (2026-05-19). They are not a fallback or cross-check.

Scope: **Fenix Android only** (confirmed 2026-05-19). iOS is out of scope until the marketing team produces an analogous CSV for iOS.

## Notebooks

| Notebook | Purpose | Status |
|---|---|---|
| `01_signal_extraction.ipynb` | Load CSV, plot weekly values for visual cross-check against STMO 118452 dashboard, document caveats, export cleaned weekly parquet | TBD |
| `02_forecast_projection.ipynb` | Weekly → daily linear interpolation, anchor at 2026-04-06, full A/B/C validation against April mktg-off forecast + June actuals, export artifact + `.meta.json` sidecar | TBD |

The structure mirrors the convolution-model worktree's `03_forecast_projection.ipynb` — same A/B/C validation framing — but with no scale-factor fit (the CSV provides a direct lift signal).

## Interpolation method

- **Linear between Monday anchors.** Each CSV row's `week` date is a Monday; the daily series is linearly interpolated between consecutive Mondays. The final 3 days after 2026-12-28 are forward-filled.
- **Anchor at campaign launch.** Subtract `total_paid_dau` at 2026-04-06 from every daily value so that day is exactly 0. Pre-launch days are clipped to 0 (the campaign hadn't started yet, so lift is definitionally zero, not negative).
- **Padding to coverage.** The export parquet covers 2026-02-01 → 2026-12-31 to match the existing convolution-model parquet schema. Days before 2026-01-05 (CSV's first Monday) are 0.

## Validation framework (in `02_forecast_projection.ipynb`)

Three diagnostic views, mirroring the convolution model's `03_forecast_projection.ipynb`:

- **Validation A — fit vs gap.** Does `marketing_lift_ma` match the empirical `gap_ma = (June_actuals_ma − April_forecast_ma)` over the post-campaign, non-Easter window? Report residual mean and RMSE. *Unlike the convolution model, the residual mean here is NOT zero by construction — it's a genuine signal.*
- **Validation B — actuals overlay.** Does `(April_forecast_ma + marketing_lift_ma) ≈ actuals_ma` post-launch?
- **Validation C — counterfactual.** Does `(actuals_ma − marketing_lift_ma) ≈ April_forecast_ma`? Report counterfactual residual mean / std / min / max.

Promotion to `data-official/2026-06/marketing/` is gated on these visuals.

## Caveats (carried into `.meta.json`)

1. **Two parallel "Paid DAU" definitions.** The marketing-team CSV uses one definition; STMO 118452 (DS team) uses cohort attribution. They should agree in magnitude but are NOT the same source.
2. **Channel filter unknown.** STMO 118452 has `normalized_channel = 'release'` commented out (all channels). The CSV's filter choice is undocumented — magnitudes will surface if it differs from the convolution model's release-only assumption.
3. **Ex-Iran assumed.** The CSV is assumed to exclude Iran (`fa` locale + IR country), matching the convolution model's baseline. Magnitudes will surface a violation if it's wrong.

## Conventions

- All notebook cells named `# [cell-name]` for `nb_cells.py` edits.
- `data/` is gitignored — cached intermediates regenerate from `01_signal_extraction.ipynb`. The `.parquet` artifact and `.meta.json` sidecar produced by `02_forecast_projection.ipynb` live in `data/` until promoted.
- When the model is ready to ship, copy `data/marketing_lift_model.real_data.<date>.parquet` + `.meta.json` to `data-official/{YYYY-MM}/marketing/` and bump `marketing.json:data_file`. The `!data-official/*/marketing/*.parquet` gitignore exception lets it land in version control.
- Plan file: `~/.claude/plans/kind-crafting-sundae.md` (approved 2026-05-19).
