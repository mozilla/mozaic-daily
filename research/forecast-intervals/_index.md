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

The code lives on `august-forecast` as `src/mozaic_daily/intervals.py` +
`scripts/compute_forecast_intervals.py` (tests in `tests/test_intervals.py`); each sub-directory keeps a
`code_snapshot/` of the version that produced it. Port the module into this branch's `src/` when a
September build needs bands — do not run the snapshot from here.

## Sub-directories

| dir | build | what it answers |
|---|---|---|
| `august-2026-desktop/` | August g01 desktop at the 2026-08-02 seam, **all adjustments off** | What the August desktop model says on its own, and how wide its own uncertainty is. Raw Dec-15 28d-MA 49,935,359; 90% band 45,007,113 – 54,629,301 |

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
