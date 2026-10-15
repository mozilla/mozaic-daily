# LOCK — 2027 desktop pre-draft: the headline forecast and the validation build

## Headline (the "best" forecast) — promoted 2026-10-14 (Brendan: "promote that to the new best forecast")

| | |
|---|---|
| build | **real-Glean MAU, additive, Linux bot overlay** — `comparison_glean_real_mau_additive_b_2026-10-14/cps0.005_thresh032_recent17_cpr0.8_ncp3_clip0.6_sps0.00825_regimeadditive/mozaic_daily_forecast.2026-10-10.gh-M.adj-bo.parquet` |
| what it is | the same mozaic model on the **real Glean desktop telemetry we actually have** (`glean_telemetry.active_users_aggregates`, Firefox Desktop, country × OS, 2025-05-22 → 2026-10-09, 17 months), **uncorrected** except where the forecast was implausible without it (Brendan, 2026-10-14) |
| seam / training end / horizon | 2026-10-10 / 2026-10-09 / 2027-12-31; **MAU only** (Brendan, 2026-10-13: "switch to MAU only") |
| model | re-tuned for a 17-month history over three probe rounds (PROCEDURE §13): **additive** seasonality regime, changepoint prior scale **0.005**, **3** changepoints, changepoint range **0.8**; otherwise the g01 lock (recent 17, sps 0.00825, thresh −0.032, radius 5/3, floor −0.6). Centre of a stable region (155.7–158.9M before the overlay) |
| corrections in the build | **Linux bot-like component `b`** (hosting-ISP clients measured from real Glean client data, ISP behavioural share ≥ 0.8; subtracted from Linux training rows, added back **held flat at 4,212,828 MAU** = the last-28-day level — a no-growth planning path, Brendan 2026-10-14); China MozillaOnline correction `o` (as in build 5); 2025 Legacy-outage calendar **off** (`--no-outage-calendar`; real Glean has no outage); Iran fill in Glean-equivalent units passed through (DAU ratio within 1%, not regenerated) |
| deliberately NOT corrected | the pre-Fx-148 duplicate-id inflation (the Dec-2025 → Jan-2026 step, ~8M, repeats in Jan 2027 as learned seasonality); its corrected twin is a **secondary comparison**, not the headline |
| headline | **world MAU 2027-12-15 = 161,546,764** (2026-12-15 166,290,298, −2.9%). Per OS: windows 128,872,745 (−5.0% Dec→Dec), mac 11,571,194 (+3.1%), linux 21,102,825 (+9.1%) — per-OS figures are flattered by mozaic's top-down reconciliation; the world number is the one to quote |
| non-bot view | minus the held add-back: world **157.3M**, which equals the Glean-equivalent build 5 (157.2M) — the independent check that the two histories agree once the one measured short component is removed |
| seam | 7-day-mean step −1.49% world MAU (windows −1.00%, mac −0.12%, linux −5.63%: the bot curve's October spike sits above its 28-day mean); soft constraint (MAU 2027-12-15 < 2026-12-15) passes |
| uncertainty | no backtest is possible on 17 months; stated by analogy from build 5's backtest (MAU 431-day |error| p50 6.7% / p90 9.9%), plus the round-3 neighbourhood (±1.6M) and the bot-path range (0 → +4.2M → growth) |
| status | PRE-DRAFT. Unlocking means a new build directory and a new row here |

## Validation build — build 5 on Glean-equivalent history (locked 2026-10-12, headline until 2026-10-14)

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
