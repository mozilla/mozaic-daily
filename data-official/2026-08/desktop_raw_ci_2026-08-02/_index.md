# `desktop_raw_ci_2026-08-02/` — August desktop, **raw model** (no adjustments) with prediction intervals

Built 2026-09-08 on the `august-forecast` branch, after the cycle was buttoned down. **Not a canonical
build and not a replacement for anything published.** It answers one question: what does the August
desktop model say on its own, with every adjustment off, and how wide is its own uncertainty?

## What it is

The canonical August desktop config (**g01**: `cps 0.1649, cpr 0.814, ncp 40, recent 17,
sps 0.00825, regime multiplicative`, holiday knobs at defaults) re-run at the published seam
**2026-08-02** (trained through 2026-08-01) on the **same cached raw pull** the canonical build used
(`raw_cache/`, byte-copied from `../desktop_g01_2026-08-02/`), with:

- `l` launch-at-login and `o` MozillaOnline **off** (`--no-launch-on-login --no-mozillaonline`), so
  Prophet trains on raw actuals and nothing is added back;
- `h` Win10 headwind **not applied** (it is display-layer and never enters a parquet);
- the in-package Iran counterfactual fill **on** (it is part of the mozaic package, not a registered
  adjustment; every 2026-07+ build uses it).

So the difference between this and `../desktop_g01_2026-08-02/` is exactly "adjustments off", nothing
else. Reproduction command is in the notebook and in `parameters.json`.

## Headline (28d-MA, 2026-12-15, desktop world)

| series | value |
|---|--:|
| raw model point forecast (trailing mean of the median path) | **49,935,359** |
| raw model band centre (median of per-path trailing means) | 49,916,850 |
| 50% interval | 48,934,166 – 51,028,125 (±1,046,979) |
| 80% interval | 46,705,607 – 53,004,274 (±3,149,333) |
| 90% interval | 45,007,113 – 54,629,301 (±4,811,094) |

Summer-trough minimum of the raw 28d-MA median: 45,305,971 on 2026-08-25.

For context only, not shown on any chart or in any CSV (dropped 2026-09-08 because markers that close to
the curve read as a comparison this build was not made for): the published August desktop figure with
`h` removed is 50,018,443, so `l`+`o` contribute +83,084 on this config.

**The two "median" numbers differ by 18,509** and that is expected: the trailing mean of the
median path is not the median of the trailing means when the path distribution is skewed. Quote
`point_forecast_28ma` as the point forecast (it is the published convention) and `median` only as
the band centre. Both are in every CSV row.

## What the intervals are, and are not

Prophet **predictive** intervals from the 1,000 sample paths mozaic stores per tile (`uncertainty_samples`),
propagated through reconciliation: MAP trend + simulated future changepoints + observation noise.
Taken as **quantiles of the 28-day trailing mean across paths** (actuals spliced in front of every
path so the window is defined from the seam), never as the trailing mean of per-day quantiles, which
overstates width. The median path reproduces the parquet's world forecast to 0.0000 DAU
(asserted before anything is written). They are **not calibrated** against realised forecast error
and say nothing about the uncertainty of the adjustments themselves.

## Files

```
desktop_raw_ci_2026-08-02/
  _index.md                           this file
  raw_desktop_intervals.ipynb         rerunnable: reruns the interval script, prints the table, draws the charts
  desktop_raw_summary.json            Dec-15 + trough summary with provenance (tracked)
  raw_cache/                          the 2026-08-02 raw legacy desktop pull (gitignored; also in ../desktop_g01_2026-08-02/)
  cps0.1649_..._regimemultiplicative/ the run: parameters.json, .raw. parquet + sidecar (sidecar tracked), pkl (gitignored)
  csv/desktop_raw_28ma_bands.csv      date, actuals_28ma, point_forecast_28ma, median, lower/upper 50/80/90 — 2026 only (tracked)
  csv/desktop_raw_daily_bands.csv     same on daily DAU (tracked)
  csv/desktop_raw_summary.csv         the headline table above (tracked)
  plots/desktop_raw_28ma_bands.png    full-year 28d-MA with shaded bands
  plots/desktop_raw_daily_bands.png   daily DAU, seam onward
  plots/desktop_raw_dec15_zoom.png    Nov–Dec 28d-MA zoom
```

Large files (parquet, pkl, raw cache) are archived under
`gs://moz-data-science-brwells-bucket/mozaic-daily-archive/august-2026/data-official/2026-08/desktop_raw_ci_2026-08-02/`.
A copy of the CSVs, plots, notebook and summary also lives on `september-forecast` at
`research/forecast-intervals/august-2026-desktop/` for cross-cycle use.

## How it was produced

```bash
# from the august-forecast checkout, venv active
python scripts/run_param_scan.py --forecast-start-date 2026-08-02 \
  --raw-cache-dir data-official/2026-08/desktop_raw_ci_2026-08-02/raw_cache \
  --results-dir  data-official/2026-08/desktop_raw_ci_2026-08-02 \
  --changepoint-prior-scale 0.1649 --changepoint-range 0.814 --n-changepoints 40 --recent-weeks 17 \
  --seasonality-regime multiplicative --no-launch-on-login --no-mozillaonline
python scripts/compute_forecast_intervals.py \
  --pkl <slug>/mozaic_objects.legacy_desktop.2026-08-02.pkl \
  --forecast-parquet <slug>/mozaic_daily_forecast.2026-08-02.ld-D.raw.parquet \
  --out-dir data-official/2026-08/desktop_raw_ci_2026-08-02
```

The model run took about two minutes and is deterministic (g01 reproduces exactly, see
`../desktop_g01_2026-08-02/_index.md`). Interval logic: `src/mozaic_daily/intervals.py`, tests in
`tests/test_intervals.py`.

## What isn't here

- No mobile counterpart (the request was desktop only). The same two commands work on a mobile
  build with `run_mobile_param_scan.py` and the `gm-D` parquet; `mobile_scoring.py` conventions apply.
- No intervals around the **published** build. The canonical g01 pickle is in GCS and
  `compute_forecast_intervals.py` accepts it directly, but its bands would be around the
  pre-add-back (organic-of-`l`/`o`) forecast and would need shifting by the exact overlay values.
- No `display_ma` seam splice. Plain `rolling(28)` throughout, by decision; identical from seam+27 on.
