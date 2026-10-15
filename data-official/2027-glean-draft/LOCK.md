# LOCK — 2027 pre-draft: the desktop headline forecast, its validation build, and the mobile MAU build (TOTAL = organic + paid)

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

## Mobile — THE DELIVERABLE: TOTAL mobile MAU = organic + paid (composed 2026-10-15)

| | |
|---|---|
| build | `mobile_total_mau_2026-10-10/` — `scripts/compose_mobile_total_mau.py` on the organic build below + the marketing DS team's paid plan (`paid_mau_2027/paid_mau_2027.opt_dec15_mau.pull2026-10-15.parquet`: STMO 128941 / `ahe_gmio_plan2027_mau_daily_20261014`, scenario **`opt_dec15_mau`** = the optimal spend model, attributed basis, bridged 2026-10-10..12-31 from the same run) |
| rules | history rows = raw actuals exactly; forecast paid = the plan as delivered, a level on Fenix and world only; IN + ID cap excess = **inauthentic organic**, its own column, **held flat** at 440,081 (Brendan 2026-10-15: "keep it flat, but document that choice"; `organic_mau/ios_rate_cap/IOS_RATE_CAP.md`); per-country paid by trailing-28d measured-paid share; organic band (Prophet, from the pickle) and paid band (the plan's) as delivered, total band = quadrature |
| headline | **world TOTAL mobile MAU 2027-12-15 = 66,327,324** [90% 61,554,555, 71,576,697] = authentic organic 51,545,439 + inauthentic organic 440,081 (flat) + paid 14,341,804; 2026-12-15 58,188,189; Dec→Dec +14.0% (organic +7.3%, the rest is the marketing plan: paid 9.4M at the edge → 14.8M). Flat-budget scenario would give paid 13,327,527 |
| seam | 7-day-mean step −1.4% total (organic −1.5%, paid −0.8%) |
| outputs | `mobile_total_mau_2026-10-10/csv/`, `plots/canonical/`; **`handoff/glean2027_mobile_mau_handoff/` + `.zip`** (index.html, README, `HANDOFF.md` briefing for the recipient's AI agent, mirroring the desktop package); ** `handoff/glean2027_mau_desktop_and_mobile.csv`** (desktop total + mobile total / organic / paid, never summed). The organic-only package of earlier on 2026-10-15 is in `handoff/_superseded_organic_only_2026-10-15/` |
| status | PRE-DRAFT. Neither half backtested; cap provisional; a new marketing scenario = recompose into a new directory + a new row |

## Mobile — the ORGANIC component (chosen 2026-10-15, Brendan: "pick that one")

| | |
|---|---|
| build | `mobile_organic_mau_2026-10-10/cps0.0065_thresh055_recent13_cpr0.87_ncp25_clip0.6_sps0.1/mozaic_daily_forecast.2026-10-10.gm-M.raw.parquet` (= round-3 probe `probes_mobile_mau/round3_ioscap/cps0.0065_cpr0.87/`, where the pickle and run log stay) |
| what it is | **organic** mobile MAU (Brendan 2026-10-15: we only forecast organic; marketing forecasts paid): real Glean mobile MAU, four apps × 15 markets + ROW, 2020-12-31 → 2026-10-09; Fenix training rows × the measured organic MAU share (`organic_mau/fenix_paid_organic_mau.2026-10-09.parquet`, IR excluded, 21.3% paid at the edge), **nothing added back**; history rows in the output are organic too |
| seam / training end / horizon | 2026-10-10 / 2026-10-09 / 2027-12-31; MAU only |
| model | October 2026 mobile lock with **changepoint prior scale 0.0065** and **changepoint range 0.87** (ncp 25, recent 13, sps 0.1, regime auto, thresh −0.055, radius 5/3, floor −0.6); production `MobileEvents` calendar |
| corrections in the build | Iran rolling-28 MAU fill (scale 1.0, `iran_fill_mobile/`); **India + Indonesia iOS-rate cap, PROVISIONAL** (organic Fenix from 2026-04-01 capped at the country's own Firefox iOS growth; 440,081 MAU removed on 2026-10-09; `organic_mau/ios_rate_cap/IOS_RATE_CAP.md` — rollback = rerun without `--ios-rate-cap` into a new directory) |
| adjustments | none; no `p` add-back by design |
| headline | **world organic mobile MAU 2027-12-15 = 51,545,439** (2026-12-15 48,049,658, +7.3% Dec→Dec). Per app 2027-12-15: Fenix 35,717,368 (+6.2%), Firefox iOS 13,433,603 (+9.1%), Focus Android 1,428,514 (+19.3%), Focus iOS 965,954 (+5.3%); the all-apps figure is the one to quote |
| seam | 7-day-mean step −1.5% world (Fenix −2.3%); the last organic actual 46,986,901 (capped series; 47,426,982 uncorrected). No "must not grow" constraint on mobile |
| why this one | PROCEDURE §16 / `probes_mobile_mau/round3.html`: on the capped series the probes form a smooth seam-vs-slope frontier (~2 pt of Dec→Dec per pt of seam); 0.0065/0.87 is marginally better than 0.005/0.87 on both and keeps 2027 growth (+7.3%; Oct→Oct +6.0%) under the capped data's own last year (+6.9%). 0.008/0.87 (seam −0.8%, +8.9%) was the seam-first alternative |
| uncertainty | not backtested yet (history long enough; next step). The frontier itself is the honest band: 0.80 → 48.7M, 0.90 at the same prior → 53.9M |
| outputs | `mobile_organic_mau_2026-10-10/csv/`, `plots/`, `plots/canonical/`, `intervals/` (Prophet band from the pickle: 90% ±4.6M on the 28d mean at 2027-12-15). Not a deliverable on its own: it is the organic half of the total above (the organic-only handoff of 2026-10-15 is in `handoff/_superseded_organic_only_2026-10-15/`; the 2026-10-14 total-MAU model package in `handoff/_superseded_total_mau_2026-10-14/`) |
| status | PRE-DRAFT. The cap is under review with the marketing DS team; reversing it means a new build directory and a new row here |

