# Marketing-lift model

Self-contained workspace for modeling the DAU lift from the Fenix Android paid-marketing campaign that launched ~2026-04-06. Output is a forecast-ready projection of marketing-attributable DAU through the forecast horizon.

Lives outside `src/mozaic_daily/` because this is exploratory modeling, not part of the production forecast pipeline. If a stable projection emerges, the relevant pieces can be promoted into the main pipeline as an adjustment series.

## Approach

Option B from the modeling discussion: **cohort convolution**. Marketing-driven DAU at day `d` is the convolution of `excess_new_profiles(t)` with a retention curve `retention(d − t)`. The retention curve is **fitted to match observed DAU lift** rather than imported from historical norms — this guarantees the fit is internally consistent and avoids the iteration trap of "build retention, project, compare, adjust retention, repeat."

## Notebooks

| Notebook | What it does | Status |
|---|---|---|
| `01_signal_extraction.ipynb` | Pulls 2025+2026 NP / DAU from BigQuery, splits by acquisition bucket (Paid Playstore / Other Playstore / Other Android), computes YoY-corrected baselines and marketing-attributable excess. Saves cached parquets to `data/` for downstream notebooks. | Working |
| `02_retention_fit.ipynb` | Loads cached signal data. Defines parametric retention curve (3-param fast/slow exponential). Fits via `scipy.optimize` such that `excess_NP ⊛ retention ≈ observed_DAU_lift`. Diagnostic panels: residuals, retention shape sanity, in-sample/holdout RMSE. | TBD |
| `03_forecast_projection.ipynb` | Takes the fitted retention curve, extends `excess_NP` forward under user-specified campaign-continuation scenarios (constant / linear decay / step-off), convolves forward to produce DAU lift projection. Combines with mozaic baseline to give total-DAU forecast scenarios. | TBD |

## Data flow

```
BigQuery
   ↓ (01_signal_extraction)
data/
  ├── new_profiles_2026.parquet, new_profiles_2025_aligned.parquet
  ├── dau_2026.parquet,           dau_2025_aligned.parquet
  └── (later) retention_curve.parquet     ← output of 02
   ↓ (02_retention_fit, 03_forecast_projection)
Forecast scenarios
```

Parquets in `data/` are gitignored — re-run `01_signal_extraction.ipynb` to regenerate.

## Key findings so far (from 01)

Ex-Iran, ex-`fa` locale; release channel only; baseline window 2026-02-01 → 2026-03-28; campaign-launch 2026-04-06:

| Bucket | Baseline YoY | Post-campaign YoY | Marketing-attributable Δ |
|---|---|---|---|
| Paid Playstore | +480k | +598k | **+117k** |
| Other Playstore | +1,017k | +1,258k | **+241k** |
| Other Android | −448k | −443k | +5k (control) |
| **Total** | **+1,050k** | **+1,413k** | **+363k** |

Other Android being a clean control (Δ ≈ 0) validates that the 8-week YoY baseline captures the structural trend faithfully — the ex-Iran filter was critical to make this work.

## Conventions

- All cells named `# [cell-name]` for use with `nb_cells.py`.
- BigQuery via `bq_query.py` (validation + SELECT-only enforcement) for one-offs; via `google.cloud.bigquery` inside notebooks.
- Use 52-week (364-day) alignment for YoY comparisons to preserve weekday-of-week.
