# `research/forecast-intervals/` — prediction intervals around forecast builds

Cross-cycle home for uncertainty bands on mozaic builds. Each sub-directory is one build's worth of
bands, copied here from the cycle branch where the model actually ran, so September and later work
can quote them without checking out an older branch.

## Method (shared by every sub-directory)

Every fitted `mozaic.Mozaic` keeps Prophet's 1,000 predictive sample paths per tile; reconciliation
shifts them by a per-day constant, so the world total still has 1,000 coherent paths. Bands are
**quantiles of the 28-day trailing mean across paths** (actuals spliced in front of every path),
never the trailing mean of per-day quantiles, which overstates width. The median path is asserted to
reproduce the parquet's point forecast before anything is written. They are Prophet *predictive*
intervals (MAP trend + simulated changepoints + observation noise) and are **not calibrated** against
realised error. Two "centre" numbers are always written: `point_forecast_28ma` (trailing mean of the
median path, the published convention) and `median` (median of the per-path means); they differ under skew.

The code is on this branch since 2026-09-10 (ported from `august-forecast`): `src/mozaic_daily/intervals.py` +
`scripts/compute_forecast_intervals.py` (tests in `tests/test_intervals.py`). The August sub-directory keeps a
`code_snapshot/` of the version that produced it; the September ones do not need one. The September builds share one
review notebook, `september_2026_raw_intervals.ipynb` (copied from `data-official/2026-09/`), with its two-panel chart
`september_2026_raw_intervals_28ma_bands.png`. The script reads the
platform from the parquet name and selects the right world row (`ld-D` → `{"os": "ALL"}`; `gm-D` → `{}` +
`app_name == "ALL MOBILE"`).

## Sub-directories

| dir | build | what it answers |
|---|---|---|
| `august-2026-desktop/` | August g01 desktop at the 2026-08-02 seam, **all adjustments off** | What the August desktop model says on its own, and how wide its own uncertainty is. Raw Dec-15 28d-MA 49,935,359; 90% band 45,007,113 – 54,629,301 |
| `september-2026-desktop/` | September g01 desktop at the 2026-09-09 seam, **all adjustments off** (`i j l o` disabled, `h` not applied) | Same question for September. Raw Dec-15 28d-MA 50,326,587; 90% band 48,477,503 – 52,425,191. Built 2026-09-10 |
| `september-2026-mobile/` | September cpr0.725 mobile at the 2026-09-09 seam, paid split `p` **off** (total-DAU fit, not organic), `t`/`u` not applied | First mobile bands. Raw Dec-15 28d-MA 17,706,153; 90% band 17,486,488 – 17,966,102 — 1.35% half-width, the model's own narrowness. Built 2026-09-10 |

## What isn't here

- The model runs and pickles. Those stay on the cycle branch (`data-official/{YYYY-MM}/...`) and in the
  GCS archive; this directory holds only the CSVs, plots, summary JSON, the executed notebook, and the
  run's `parameters.json` + sidecar meta for provenance.
- Any calibration or backtest of the bands against realised actuals. That would be a new sub-directory
  or a new topic (`research/csv-vs-actuals/` is the nearest existing one).

## Where new work goes

One sub-directory per build: `{cycle}-{platform}[-{variant}]/` with `csv/`, `plots/`, the summary JSON,
the notebook, provenance files, a `code_snapshot/`, and an `_index.md` stating the seam, config, which
adjustments were on, and the headline numbers.
