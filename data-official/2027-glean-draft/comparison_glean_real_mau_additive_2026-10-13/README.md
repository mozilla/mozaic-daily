# `comparison_glean_real_mau_additive_2026-10-13/` — the real-Glean MAU comparison build (adopted "temporarily", Brendan 2026-10-13)

The same model on **real Glean desktop telemetry only** (2025-05-22 → 2026-10-09, no simulated history), **MAU only**, with the
parameters re-tuned for 17 months of data after three probe rounds (`../probes_glean_real_mau/`, `scores_round{1,2,3}.csv`):

| | locked build | this build |
|---|---|---|
| seasonality regime | multiplicative | **additive** |
| changepoint prior scale / count / range | 0.1649 / 40 / 0.87 | **0.005 / 3 / 0.8** |
| 2025 Legacy-outage calendar | on (the simulated history inherits the outage) | **off** (`--no-outage-calendar`; real Glean has no outage) |
| Iran fill, China correction | on | on (fill in Glean-equivalent units, see COMPARISONS.html §2) |

The parquet is the probe `../probes_glean_real_mau/cps0.005_ncp3_cpr0.8_additive/cps0.005_thresh032_recent17_cpr0.8_ncp3_clip0.6_sps0.00825_regimeadditive/mozaic_daily_forecast.2026-10-10.gh-M.adj-o.parquet`
(+ sidecar, pickle); this directory holds its exported `csv/` and `plots/` (`scripts/export_glean_history_curves.py --stem comparison_glean_real_mau`).
World MAU 2027-12-15 **158,436,071** (2026-12-15 163,275,724; −3.0%). Why these parameters and not others: COMPARISONS.html §3.
Still open: the Linux bot-like surge is extrapolated as trend (Linux 19.9M → 21.9M); a measured short-component overlay is the next round.
