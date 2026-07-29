# Phase 0–1 — center re-measure, and the `regime=multiplicative` probe

Scored with `scripts/score_near_horizon.py` at the **Aug-25 trough minimum** (not July's Aug-22 — see
"Scoring changes" below), post-headwind, against `data-official/2026-08/adjustments/headwind.json`
(−1,245,000, ramping from the 2026-07-28 seam).

## Result

| build | trough (Aug-25) | Dec-15 | seam slope kink (model) |
|---|--:|--:|--:|
| center, `regime=auto` | 43,833,674 | 48,672,970 | −72,593/day |
| **`regime=multiplicative`** | **45,140,569** | **48,925,520** | **−23,197/day** |
| Δ | **+1,306,895** | **+252,550** | **+49,396 (68% smaller)** |

Trade ratio **5.17 : 1** (trough gain per unit of Dec-15 drift). The trough minimum stayed on 2026-08-25
under both regimes — the shape change did not move the argmin.

**In band.** The 45M–46M August target band is met at 45,140,569 (+140,569 over the 45M floor) in a single
probe. The two configs differ in `seasonality_regime` and nothing else (verified by diffing
`parameters.json`; slug string aside, one line).

### The §5 hypothesis was correct

Predicted ~45.2M on the reasoning that July's +1.29M was measured from a 43.25M center and ~0.47M of this
build's higher center is display-layer, so the regime's model-level effect should still be fully
available. Landed at 45.14M — 60K under prediction, and the gain itself came in at +1.31M vs July's
+1.29M, i.e. it transferred almost exactly. July's "plateaued ~44.54M" ceiling was a property of that
lower baseline, not of the regime.

### Seam derivative (reported, not scored)

14-day OLS fit either side of the seam on the display 28d-MA:

| | before (actuals) | after | kink |
|---|--:|--:|--:|
| center, model only | −52,790 | −125,383 | −72,593 |
| multiplicative, model only | −52,790 | −75,987 | **−23,197** |
| headwind contribution | 0 | −8,893 | −8,893 |

Multiplicative improves the handoff derivative substantially: the model's own slope kink drops 68%, from
1.4× steeper than the actuals' decline to 0.44× steeper. This is a *side benefit* — the probe was not
tuned for it.

The **−8,893/day headwind contribution is not parameter-addressable.** The re-anchored ramp removed the
seam *level* step but necessarily introduces a slope kink of exactly `desktop_dau / (anchor − start)`
= −1,245,000 / 140 days, since the ramp contributes zero slope before the seam and a constant slope after
it. That is a floor on the achievable display-curve kink.

## Long-horizon check — no growth-mode artifact

On desktop the regime is coupled to growth (`additive → logistic`, `multiplicative → linear`), so
switching it also disables the 426-day logistic cap — which has a known artifact history (see the
`logistic-cap-artifact` note). Checked for a runaway; there isn't one:

| date | auto | multiplicative | Δ | Δ% |
|---|--:|--:|--:|--:|
| 2026-08-25 | 44,082,674 | 45,389,569 | +1,306,895 | +2.96% |
| 2026-10-01 | 48,373,593 | 49,050,702 | +677,108 | +1.40% |
| 2026-12-15 | 49,917,970 | 50,170,520 | +252,550 | +0.51% |
| 2027-03-01 | 48,860,812 | 49,043,916 | +183,103 | +0.37% |
| 2027-06-15 | 46,493,131 | 47,022,845 | +529,714 | +1.14% |
| 2027-08-25 | 41,145,330 | 43,164,060 | +2,018,730 | +4.91% |
| 2027-12-15 | 47,094,377 | 47,679,493 | +585,115 | +1.24% |
| 2027-12-31 | 42,850,666 | 43,569,405 | +718,739 | +1.68% |

(Pre-headwind `display_ma`, hence the Dec-15 figures differ from the post-headwind table above.)

Multiplicative runs uniformly *slightly* above auto and **converges** at both Decembers rather than
diverging. Its effect is concentrated in the summer troughs of both years (+2.96%, +4.91%) — it makes
summer shallower, which is the intended direction, and does so consistently rather than as a one-off
2026 artifact. Implied Aug-to-Aug YoY: auto −6.66%, multiplicative −4.90%; both decline.

⚠️ Not checked: whether the shallower summer is *right*. This is a shape argument, and the long-horizon
table shows the regime is self-consistent, not that it is more accurate. A backtest against 2024/2025
summer actuals would settle it and has not been run.

## Scoring changes made during phase 0

Three defects found and fixed in `scripts/score_near_horizon.py`:

1. **Stale `DEFAULT_HEADWIND`** pointed at `data-official/2026-07/` — would have scored every probe
   against July's −1,345,000 amplitude *and* July's 2026-04-01 ramp start, silently.
2. **Wrong MA convention.** The scorer used a plain `rolling(28).mean()`; the canonical notebook uses the
   variance-matched `display_ma` splice. These agree past the seam zone but disagree by **+41,189** at
   Aug-22, which sat *inside* the 27-day transition window. The scorer now imports `display_ma` (same
   precedent as `scripts/mobile_sensitivity.py`) and reproduces the notebook exactly at both dates.
3. **Target date moved to the trough minimum, 2026-08-25.** Aug-22's value was 41K sensitive to the
   smoothing convention; Aug-25 is exactly 28 days past the seam, so its window is entirely forecast and
   the value is convention-independent (plain and spliced agree to the cent — verified).

Also: `TARGET_BULLSEYE`/`TARGET_TOL` (July's 45.06M ±0.1M) replaced by `TARGET_BAND = (45M, 46M)`. The
45.06M figure was never an external benchmark — it was the most achievable value under July's data, and
is retired. Trough-argmin reporting and the seam-derivative report were added.

Tests: `tests/test_score_near_horizon.py` now 6 passing. The two synthetic tests pass `forecast_start`
explicitly; the July regression test pins both July's spec and Aug-22 (it was silently following mutable
module defaults, which is why it broke on the repoint); one new test fails if the MA reverts to a plain
rolling window, and one covers deriving the seam from the parquet column.

## Open — needs a decision before phase 2

Dec-15 drifts **+252,550 (+0.52%)** high. "Stable" was the stated requirement, so this likely needs
clawing back via `cpr` or a bounded `sps` move (§6 step 2). Not attempted — stopping here as instructed.

## Reproduce

```bash
source .venv/bin/activate
python scripts/run_param_scan.py \
  --forecast-start-date 2026-07-28 \
  --raw-cache-dir data-official/2026-08/desktop_baseline_2026-07-28/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825 \
  --results-dir research/param-scans/summer-trough-v2/phase1 \
  --changepoint-prior-scale 0.08983 --changepoint-range 0.65 --n-changepoints 25 \
  --recent-weeks 13 --seasonality-prior-scale 0.00825 --seasonality-regime multiplicative \
  --holiday-threshold -0.032 --holiday-max-radius 5 --holiday-min-radius 3 \
  --holiday-effect-floor -0.6
```

Requires the sibling mozaic checkout on `configurable-model-params` (`126fe14`, `6f02912`) for
`seasonality_regime`. ~13 min; BigQuery not re-queried (raw cache symlinked).
