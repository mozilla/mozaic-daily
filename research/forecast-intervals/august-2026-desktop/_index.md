# `august-2026-desktop/` — August g01 desktop, all adjustments off, with prediction intervals

Copied 2026-09-08 from `august-forecast:data-official/2026-08/desktop_raw_ci_2026-08-02/` (commit on that
branch titled "August desktop raw model (all adjustments off) with prediction intervals"). The
model ran there, in a worktree, on the cached 2026-08-02 raw pull; nothing here was produced on
`september-forecast`. **Not canonical, changes no published August number.**

## Build

- Config **g01** (`cps 0.1649, cpr 0.814, ncp 40, recent 17, sps 0.00825, regime multiplicative`,
  holiday knobs default) — `parameters.json`.
- Seam **2026-08-02**, trained through 2026-08-01, same raw rows as the published build.
- `l` launch-at-login and `o` MozillaOnline **off**; `h` Win10 headwind **not applied**; in-package Iran
  fill **on**. Sidecar: `mozaic_daily_forecast.2026-08-02.ld-D.raw.parquet.meta.json` (adjustments: none).

## Headline — desktop world 28d-MA, 2026-12-15

| series | value |
|---|--:|
| raw point forecast (trailing mean of the median path) | **49,935,359** |
| raw band centre (median of per-path trailing means) | 49,916,850 |
| 50% | 48,934,166 – 51,028,125 (±1,046,979) |
| 80% | 46,705,607 – 53,004,274 (±3,149,333) |
| 90% | 45,007,113 – 54,629,301 (±4,811,094) |

Raw 28d-MA trough: 45,305,971 on 2026-08-25. Published references were deliberately left off the
charts and CSVs (for context: published-minus-`h` is 50,018,443, so `l`+`o` contribute +83,084 here).

Band width grows roughly linearly through the horizon: 90% width is 660,882 on Sep 1, 2.18M on Oct 1,
4.65M on Nov 1, 9.62M on Dec 15 (from the notebook's `[plot-zoom]` cell).

## Files

```
csv/desktop_raw_28ma_bands.csv     date, actuals_28ma, point_forecast_28ma, median, lower/upper 50/80/90 (2026)
csv/desktop_raw_daily_bands.csv    same on daily DAU
csv/desktop_raw_summary.csv        the table above
desktop_raw_summary.json           summary with provenance (config, adjustments, method, n paths)
plots/desktop_raw_28ma_bands.png   full-year 28d-MA with shaded bands
plots/desktop_raw_daily_bands.png  daily DAU, seam onward
plots/desktop_raw_dec15_zoom.png   Nov–Dec 28d-MA zoom
plots/desktop_raw_28ma_bands.notebook.png   the notebook's rendering (adds the dashed point-forecast line)
raw_desktop_intervals.ipynb        executed copy; RUNS ONLY on august-forecast (needs the pickle + that branch's src)
parameters.json, *.meta.json       provenance of the run
code_snapshot/                     intervals.py + compute_forecast_intervals.py as used; reference only
```

Model artifacts (parquet, 628 MB pickle, raw cache):
`gs://moz-data-science-brwells-bucket/mozaic-daily-archive/august-2026/data-official/2026-08/desktop_raw_ci_2026-08-02/`.
