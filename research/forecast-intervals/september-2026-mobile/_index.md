# `mobile_raw_ci_2026-09-09/` — September mobile, **raw model** (no paid treatment) with prediction intervals

Built 2026-09-10 on `september-forecast`, the first mobile interval build (August had desktop only).
**Not a canonical build and not a replacement for anything published.** It answers: what does the
September mobile model say on its own, with the paid treatment off, and how wide is its own uncertainty?

## What it is

The live September mobile config (**cpr 0.725**, unchanged from August: `cps 0.035, cpr 0.725, ncp 25,
recent 13, sps 0.1, regime auto, holiday threshold −0.055`) re-run at the refreshed seam **2026-09-09**
(trained through 2026-09-08) on the **same cached raw pull** the canonical build used
(`../mobile_rawpull_2026-09-09/`, symlinked), with:

- the paid/organic split `p` **off** (`--no-organic-split`, added to `run_mobile_param_scan.py` for this
  build). **This is a TOTAL-DAU mobile forecast with no paid treatment at all, not an organic one**:
  Prophet fits raw Fenix DAU including paid users and extrapolates them like everything else. The
  canonical build instead fits organic DAU and stacks the marketing team's paid level back on;
- `t` mobile calibration tailwind (+299,000) and `u` terms-of-use headwind (−27,162) **not applied**
  (display layer; never enter a parquet);
- the in-package Iran counterfactual fill **on**.

## Headline (28d-MA, 2026-12-15, ALL MOBILE world)

| series | value |
|---|--:|
| raw model point forecast (trailing mean of the median path) | **17,706,153** |
| raw model band centre (median of per-path trailing means) | 17,702,093 |
| 50% interval | 17,656,474 – 17,751,841 (±47,684) |
| 80% interval | 17,566,833 – 17,841,561 (±137,364) |
| 90% interval | 17,486,488 – 17,966,102 (±239,807) |

For context only, kept off every chart and CSV: the live `.adj-p.` build's Dec-15 28d-MA before the display
layer is 17,925,840, so the paid treatment `p` (organic fit + lower-bound paid level) sits **+219,687**
above the total-DAU fit on this config; the published mobile figure is 18,197,678 (that number plus `t`
and `u`).

**The mobile band is very narrow: the 90% half-width is 1.35% of the level**, against 3.9% for the
September desktop build and 9.6% for August desktop. That is what the mobile config produces (a
changepoint prior a fifth of desktop's, and reconciliation top-down from one aggregate fit), not a
property of mobile DAU; treat it as the model's self-reported uncertainty and nothing more. Prophet
intervals are not calibrated.

The `trough_28ma` field (17,298,375 on 2026-09-30) is **not meaningful**: the default window
(2026-08-01 – 2026-09-30) ends at 09-30 while the median keeps falling into early October.

## What the intervals are, and are not

Same method as desktop: Prophet **predictive** intervals from the 1,000 stored sample paths, taken as
**quantiles of the 28-day trailing mean across paths** with actuals spliced in front; the median path
reproduces the parquet's `ALL MOBILE` world forecast to 0.0000 DAU. The world row on mobile is
`country == "ALL"`, `segment == "{}"`, `app_name == "ALL MOBILE"` (the parquet also carries one world row
per app; `compute_forecast_intervals.py` selects on the app since this build). Not calibrated; plain
`rolling(28)`, no `display_ma`.

## Files

```
mobile_raw_ci_2026-09-09/
  _index.md                           this file
  run.log                             the model run's stdout
  (notebook)                          see ../september_raw_intervals.ipynb — one notebook covers both platforms' builds
  mobile_raw_summary.json             Dec-15 (+ the non-meaningful trough) summary with provenance (tracked)
  cps0.035_thresh055_recent13_cpr0.725_ncp25_clip0.6_sps0.1/
                                      the run: parameters.json (adjustment_code null), .raw. parquet + sidecar
                                      (sidecar tracked), pkl (gitignored), symlink to the raw pull
  csv/mobile_raw_28ma_bands.csv       date, actuals_28ma, point_forecast_28ma, median, lower/upper 50/80/90 — 2026 only (tracked)
  csv/mobile_raw_daily_bands.csv      same on daily DAU (tracked)
  csv/mobile_raw_summary.csv          the headline table above (tracked)
  plots/mobile_raw_28ma_bands.png     full-year 28d-MA with shaded bands
  plots/mobile_raw_daily_bands.png    daily DAU, seam onward
  plots/mobile_raw_dec15_zoom.png     Nov–Dec 28d-MA zoom
```

Large files go to GCS at the button-down under
`.../mozaic-daily-archive/september-2026/data-official/2026-09/mobile_raw_ci_2026-09-09/`. A copy of the
CSVs, plots, notebook and summary lives at `research/forecast-intervals/september-2026-mobile/`.

## How it was produced

```bash
# september-forecast checkout, venv active; ~4 min. The mobile scan skips the BQ pre-flight when a raw cache is given.
python scripts/run_mobile_param_scan.py --forecast-start-date 2026-09-09 \
  --raw-cache-dir data-official/2026-09/mobile_rawpull_2026-09-09 \
  --results-dir  data-official/2026-09/mobile_raw_ci_2026-09-09 \
  --changepoint-prior-scale 0.035 --changepoint-range 0.725 --n-changepoints 25 --recent-weeks 13 \
  --seasonality-prior-scale 0.1 --holiday-threshold -0.055 --no-organic-split
python scripts/compute_forecast_intervals.py \
  --pkl <slug>/mozaic_objects.glean_mobile.2026-09-09.pkl \
  --forecast-parquet <slug>/mozaic_daily_forecast.2026-09-09.gm-D.raw.parquet \
  --out-dir data-official/2026-09/mobile_raw_ci_2026-09-09
```

## What isn't here

- An **organic**-only band, or a band around the published build (organic fit + paid level + `t` + `u`).
  The canonical pickle holds the organic-fit paths; adding the paid level and the display layer as
  constants would give published-build bands of the same width as the organic fit's.
- Any calibration against realised error.
