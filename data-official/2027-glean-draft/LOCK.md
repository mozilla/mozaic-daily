# LOCK — 2027 Glean-history desktop pre-draft, locked 2026-10-12 ("lock it for now", Brendan)

| | |
|---|---|
| build | **build 5** — `desktop_glean_history_cpr087_o_2026-10-10/cps0.1649_thresh032_recent17_cpr0.87_ncp40_clip0.6_sps0.00825_regimemultiplicative/mozaic_daily_forecast.2026-10-10.gh-D+M.adj-o.parquet` |
| seam / training end / horizon | 2026-10-10 / 2026-10-09 / 2027-12-31 |
| inputs | legacy-to-glean fc15 export (history to 2026-09-08) + D-118 continuation (to 2026-10-09), `glean_equivalent`, whole-people integers; export-rollups @ 22e01e2 |
| model | October 2026 g01 desktop lock with **`prophet_changepoint_range` 0.87** (was 0.814): cps 0.1649, ncp 40, recent 17, sps 0.00825, regime multiplicative, thresh −0.032, radius 5/3, floor −0.6 |
| corrections in the build | Iran counterfactual fill (scale 1.0 / 1.0); 2025 Legacy outage union holiday window; China MozillaOnline migration subtracted/added back on DAU and MAU, windows/mac split (`o`, data correction) |
| adjustments | none (no head- or tailwinds); candidates registered separately, not applied |
| headline | **world MAU 2027-12-15 = 157,222,571** (2026-12-15 160,221,373, −1.9%); DAU 28d MA 2027-12-15 52,731,308 (2026-12-15 53,891,970) |
| seam | 7-day-mean step −0.28% MAU, −0.13% DAU; soft constraint (MAU 2027 < MAU 2026) passes |
| status | PRE-DRAFT. Translation band is the only uncertainty in the report until the backtest lands |

Why 0.87 and nothing else: PROCEDURE.html §9b (changepoint range closes the seam; 0.90+ over-extrapolate the autumn ramp and
fail the soft constraint) and §9c (changepoint prior scale is insensitive). Unlocking means a new build directory and a new row here.
