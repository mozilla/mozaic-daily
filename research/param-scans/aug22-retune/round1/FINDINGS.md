# Aug-trough search — Round 1 findings (2026-07-10)

Anchor 2026-07-06, desktop legacy-DAU, `regime=auto`, headwind display-layer.
Center = locked July config (cps 0.08983, cpr 0.65, recent 13, ncp 25, sps 0.00825).
Target = Global trough(2026-08-22) 28d-MA post-headwind → **45.06M ±0.1M**. Center = 43.25M (gap −1.81M).

**Method caveat:** medium deltas → per-knob numbers are secant slopes over a medium interval,
not true local derivatives (Brendan's note). Direction-finding only.

## Result table (post-headwind Global trough; gap vs 45.06M bullseye)

| probe | trough_post | gap | Δ vs center | Dec-15 post | ex-CN/IR post |
|---|---|---|---|---|---|
| center (locked) | 43,246,576 | −1,813,424 | — | 48,585,483 | 40,782,423 |
| cps 0.075 / 0.105 | 43.21M / 43.25M | | −32K / +5K | 48.55 / 48.58M | |
| cpr 0.60 / 0.70 | 43.20M / 43.22M | | −42K / −23K | 48.54 / 48.56M | |
| recent 10 / 16 | 43.22M / 43.23M | | −29K / −18K | 48.55 / 48.56M | |
| ncp 20 / 30 | 43.22M / 43.20M | | −26K / −46K | 48.55 / 48.52M | |
| **sps 0.003** | **45,287,305** | **+227,305** | **+2,040,729** | **45,668,335** | 42,932,690 |
| sps 0.025 | 43,172,585 | −1,887,415 | −74K | 48.57M | 40,703,568 |
| sps 2.0 | 43,166,741 | −1,893,259 | −80K | 48.57M | 40,696,206 |

## Conclusions

1. **`seasonality_prior_scale` is the only lever that matters.** The four trend knobs (cps, cpr,
   recent_weeks, ncp) each move the trough by <±80K and are mostly non-monotonic around center — they
   cannot close a 1.81M gap. Confirms the pre-search hypothesis (a 28d-MA at a summer trough is driven
   by yearly-seasonality magnitude, not trend/weekly knobs).
2. **Direction = DOWN.** Lower sps → smaller yearly-seasonality amplitude → shallower summer dip →
   higher trough. sps 0.00825→0.003 lifts the trough +2.04M (43.25M→45.29M), **overshooting** the
   bullseye by +0.23M. Raising sps (0.025, 2.0) deepens the dip slightly then plateaus.
3. **Target sps ≈ 0.0035–0.0045** (between 0.003 and 0.00825; the response is steep and nonlinear at
   the low end). This is a near-1-D problem in sps.
4. **⚠ Dec-15 coupling is large.** sps is a seasonal-*amplitude* knob: flattening seasonality raises the
   summer trough AND lowers the winter peak symmetrically. sps=0.003 drops Dec-15 from 48.59M to 45.67M
   (**−2.92M**). A later headwind correction of a drop that size would back-react ~0.55× on Aug-22
   (~+1.6M) — i.e. the sps-only path buys the Aug trough at a steep Dec-15 cost that is NOT cheaply
   headwind-correctable without moving Aug-22 back out of band. Flag for the co-solve decision.
5. **⚠ Robustness / overfitting watch.** The target sits in a steep part of the sps response
   (~−390M trough per unit sps locally), so the KPI is highly sensitive to sps there — the opposite of
   the "flat, generalizable region" we prefer. Also, very low sps ≈ near-zero yearly seasonality, a
   strong structural statement. Worth weighing before locking.

## Proposed Round 2
1-D sps refine to bracket 45.06M: sps ∈ {0.0035, 0.004, 0.0045, 0.005, 0.006}, regime=auto. Report
Global + ex-CN/IR + Dec-15 for each, plus local sensitivity (Δtrough per Δsps) to quantify robustness.
Open question for Brendan: accept the Dec-15 drop (float, correct later) or co-solve sps + headwind?
