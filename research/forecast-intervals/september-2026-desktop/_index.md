# `desktop_raw_ci_2026-09-09/` — September desktop, **raw model** (no adjustments) with prediction intervals

Built 2026-09-10 on `september-forecast`, the September counterpart of
`data-official/2026-08/desktop_raw_ci_2026-08-02/` (on `august-forecast`; copied to
`research/forecast-intervals/august-2026-desktop/`). **Not a canonical build and not a replacement for
anything published.** It answers one question: what does the September desktop model say on its own,
with every adjustment off, and how wide is its own uncertainty?

## What it is

The live September desktop config (**g01**, unchanged from August: `cps 0.1649, cpr 0.814, ncp 40,
recent 17, sps 0.00825, regime multiplicative`, holiday knobs at defaults) re-run at the refreshed seam
**2026-09-09** (trained through 2026-09-08) on the **same cached raw pull** the canonical build used
(`../desktop_rawpull_2026-09-09/`, symlinked), with:

- the four per-tile overlays `i` India excess, `j` Japan bot, `l` launch at login (new users) and
  `o` MozillaOnline **off** (`--disable-adjustment i j l o`), so Prophet trains on raw actuals and nothing
  is added back;
- `h` Win10 headwind **not applied** (display layer; never enters a parquet);
- the in-package Iran counterfactual fill **on** (part of the mozaic package, not a registered adjustment).

So the difference between this and the live `../desktop_g01_2026-09-09/` is exactly "adjustments off".

## Headline (28d-MA, 2026-12-15, desktop world)

| series | value |
|---|--:|
| raw model point forecast (trailing mean of the median path) | **50,326,587** |
| raw model band centre (median of per-path trailing means) | 50,314,819 |
| 50% interval | 49,981,810 – 50,678,498 (±348,344) |
| 80% interval | 49,174,397 – 51,642,334 (±1,233,969) |
| 90% interval | 48,477,503 – 52,425,191 (±1,973,844) |

For context only, kept off every chart and CSV as in August: the live `.adj-ijlo.` build's Dec-15 28d-MA
before the display layer is 50,349,720, so `i`+`j`+`l`+`o` together contribute **+23,133** on this config;
the published desktop figure is 49,182,443 (that number less `h` at −1,167,277).

**The band is much narrower than August's.** August's 90% half-width at Dec-15 was 4,811,094 from a
2026-08-02 seam (135 days out); this is 1,973,844 from a 2026-09-09 seam (97 days out). Prophet's
predictive width grows with horizon through the simulated future changepoints, but a 2.4× drop for a 28%
shorter horizon is more than horizon alone would give; the rest is the fit itself (the later seam sits past
the summer trough, on a rising stretch). Not investigated further here.

The `trough_28ma` field in the summary JSON/CSV is **not meaningful for this build**: the default trough
window (2026-08-01 – 2026-09-30) begins before the seam, so it reports the first band day (2026-09-09,
47,327,703), which is the seam, not a trough.

The two "median" numbers differ by 11,768, as expected under a skewed path distribution. Quote
`point_forecast_28ma` as the point forecast (the published convention) and `median` only as the band centre.

## What the intervals are, and are not

Prophet **predictive** intervals from the 1,000 sample paths mozaic stores per tile, propagated through
reconciliation: MAP trend + simulated future changepoints + observation noise. Taken as **quantiles of the
28-day trailing mean across paths** (actuals spliced in front), never as the trailing mean of per-day
quantiles. The median path reproduces the parquet's world forecast to 0.0000 DAU (asserted before anything
is written). They are **not calibrated** against realised forecast error and say nothing about the
uncertainty of the adjustments themselves. Plain `rolling(28)` throughout, no `display_ma` splice.

## Files

```
desktop_raw_ci_2026-09-09/
  _index.md                           this file
  run.log                             the model run's stdout (overlay resolution lines, watchdog, timings)
  (notebook)                          see ../september_raw_intervals.ipynb — one notebook covers both platforms' builds
  desktop_raw_summary.json            Dec-15 (+ the non-meaningful trough) summary with provenance (tracked)
  cps0.1649_..._regimemultiplicative/ the run: parameters.json, .raw. parquet + sidecar (sidecar tracked), pkl (gitignored),
                                      symlink to the raw pull
  csv/desktop_raw_28ma_bands.csv      date, actuals_28ma, point_forecast_28ma, median, lower/upper 50/80/90 — 2026 only (tracked)
  csv/desktop_raw_daily_bands.csv     same on daily DAU (tracked)
  csv/desktop_raw_summary.csv         the headline table above (tracked)
  plots/desktop_raw_28ma_bands.png    full-year 28d-MA with shaded bands
  plots/desktop_raw_daily_bands.png   daily DAU, seam onward
  plots/desktop_raw_dec15_zoom.png    Nov–Dec 28d-MA zoom
```

Large files (parquet, pkl) go to GCS at the cycle button-down under
`gs://moz-data-science-brwells-bucket/mozaic-daily-archive/september-2026/data-official/2026-09/desktop_raw_ci_2026-09-09/`.
A copy of the CSVs, plots and summary lives at `research/forecast-intervals/september-2026-desktop/`.

## How it was produced

```bash
# september-forecast checkout, venv active; ~3 min
python scripts/run_param_scan.py --forecast-start-date 2026-09-09 \
  --raw-cache-dir data-official/2026-09/desktop_rawpull_2026-09-09 \
  --results-dir  data-official/2026-09/desktop_raw_ci_2026-09-09 \
  --changepoint-prior-scale 0.1649 --changepoint-range 0.814 --n-changepoints 40 --recent-weeks 17 \
  --seasonality-prior-scale 0.00825 --seasonality-regime multiplicative \
  --disable-adjustment i --disable-adjustment j --disable-adjustment l --disable-adjustment o
python scripts/compute_forecast_intervals.py \
  --pkl <slug>/mozaic_objects.legacy_desktop.2026-09-09.pkl \
  --forecast-parquet <slug>/mozaic_daily_forecast.2026-09-09.ld-D.raw.parquet \
  --out-dir data-official/2026-09/desktop_raw_ci_2026-09-09
```

Note the pre-flight BigQuery check still runs for desktop even with a raw cache, so ADC credentials must be
live. Interval logic: `src/mozaic_daily/intervals.py`, tests in `tests/test_intervals.py`.

## What isn't here

- Intervals around the **published** build. The canonical pickle's paths are pre-add-back (the overlays'
  training-row subtraction) and would need shifting by the exact overlay and `h` values.
- Any calibration against realised error.
- The mobile counterpart is `../mobile_raw_ci_2026-09-09/`.
