# Grid search — negative result: the ±50k Dec-15 constraint caps the trough gain at +16,875

24 probes, 3 concurrent, 19.3 minutes, zero failures. Report:
`../grid_report.html` (self-contained). Raw scores: `../grid_scores.csv`.

**Objective**: raise the Aug-25 trough minimum (28d-MA, post-headwind) while holding Dec-15 within
**±50,000** of the canonical 48,672,970. Headwind fixed at −1,245,000 — the ±50k allowance *is* its own
adjustability.

## Result

| | trough | Dec-15 drift | seam kink | n | in band |
|---|--:|--:|--:|--:|:--:|
| **A · additive-dominant** | 43,787,850 … 43,850,549 | −39,062 … +23,827 | −73,900 … −71,611 | 11 | all ✓ |
| **B · multiplicative-dominant** | **45,131,895 … 45,244,807** | +252,452 … +309,613 | **−23,436 … −17,298** | 15 | none ✗ |

**Best config holding Dec-15**: trough **43,850,549**, a gain of **+16,875 (+0.04%)** — effectively
nothing. `cps0.08983_thresh032_recent13_cpr0.55_ncp25_clip0.6_sps0.00825_corr-0.13`, Dec-15 48,696,797
(+23,827).

## Outcomes are bimodal, not a frontier

Every one of the 26 builds lands in one of two disjoint clusters separated by an **empty 1,281,347 gap**
in the trough and an empty **+23,827 → +252,452** gap in Dec-15 drift. There is nothing to trade along:
no config buys a fraction of the trough lift for a fraction of the Dec-15 cost.

This was not a resolution problem. The grid deliberately bracketed the boundary at four points
(−0.105, −0.13, −0.14, −0.15), and the discontinuity survives at full resolution.

### The whole step is one tile

`ROW/modern_windows` carries **27% of all desktop weight** and its corr is **−0.1465**, so it flips
atomically as the threshold crosses that value. The evidence that the effect is essentially *only* this
tile:

| threshold | DAU-weighted multiplicative | trough gain |
|--:|--:|--:|
| 0.00 (canonical) | 7.6% | — |
| −0.105 | 11.6% | +2,478 |
| −0.13 | 16.0% | +4,041 |
| −0.14 | 16.0% | +4,041 |
| **−0.15** | **48.9%** | **+1,308,440** |
| −0.26 | 78.0% | +1,307,277 |
| −0.35 | 98.0% | +1,306,921 |
| −1.00 | 100% | +1,306,895 |

Moving 11.6–16.0% of DAU to multiplicative buys 2.5–4.0K. Crossing ROW buys 1.31M. Going from there to
100% buys *nothing further* — it is flat to within 1,545 across −0.15 → −1.00.

So the summer-trough lift is not a broad seasonal-shape effect that parameters can dial. It is a single
heavy tile switching from logistic/additive to linear/log-space/multiplicative, and it is indivisible.

### Both secondary goals live in cluster B as well

The seam handoff derivative only improves in cluster B (kink −17K to −23K vs −72K to −74K). The best
in-band config reaches −71,611 against the canonical's −72,593 — a 1.4% improvement, i.e. noise. There
is no in-band config that meaningfully improves the seam slope either.

## Validation checks, both passed

- **Determinism**: thresholds −0.13 and −0.14 select an identical tile set (no tile's corr lies between
  them) and produced byte-identical outcomes — 43,837,715 / 48,672,962 for both. Confirms runs are
  reproducible and the axis behaves as specified.
- **Endpoint**: `corr=−1.00` (45,140,569 / 48,925,520) reproduces `regime=multiplicative`
  (45,140,569 / 48,925,520) exactly, and `corr=−0.35` lands within 26 DAU of it. Confirms the new knob
  spans to the forced regime correctly.

## Knobs that did not decouple the two dates

- **`holiday_threshold` −0.024** was the main hope, since Dec-15's 28d window spans Nov-18→Dec-15 and
  contains Thanksgiving while the August window is nearly holiday-free. It moves both dates in the *same*
  direction and is not even consistent in sign across clusters: in A it lowered Dec-15 by 32–39K but also
  lowered the trough by 28–46K; in B it *raised* Dec-15 drift (+274K vs +252K). No usable asymmetry.
- **`cpr` 0.55** raises both (A: trough +12,947 / Dec +23,820. B: trough +1,411,133 / Dec +304,040).
  It is the only knob that improved the in-band trough at all, and it contributed +12.9K of the +16.9K.
- **`sps`** at 0.006 / 0.00825 / 0.012 moved Dec-15 drift +268,785 / +260,623 / +256,138 — about −4,500
  per +0.004. Extrapolating to absorb 200K needs sps ≈ 0.19, a 23× move far outside any tested range and
  certain to distort the seasonal shape. Not viable.
- **`recent_weeks` 8** was inert (trough −1,905, Dec +1,817 vs its center).

## What this leaves

The +1.3M is real and reproducible, but it is only purchasable at **+252,452 minimum on Dec-15 — 5.0× the
budget**. Absorbing that with the headwind would need an anchor of **−1,497,452**, more negative than
June's −1,420,000 and reversing all four of this cycle's attenuations. That is a judgement call about the
Win10 headwind belief, not something a parameter search can settle, and it is **not** being proposed here.

Options, none of which should be actioned without a decision:

1. **Accept the negative** — trough stays ~43.85M, Dec-15 held.
2. **Relax the Dec-15 tolerance** to ~+255K and take cluster B (trough 45.13–45.24M, plus the much better
   seam slope).
3. **Re-anchor the headwind** to −1,497,452 to absorb it — reverses four attenuations; needs telemetry
   support that `research/headwinds/` may or may not provide.
4. **Bidirectional summer-trough overlay** — July's recommendation, and the only route that lifts summer
   without touching December. Requires explicit human go/no-go; deliberately not built.

## Open question worth its own look

`ROW/modern_windows` is a 12.66M-DAU bucket of many countries being handed a single regime decision on one
correlation statistic, and that decision is worth 1.3M at the summer trough. Its corr sits −0.1465 from
the production cutoff, so it is not currently marginal — but the sensitivity is concentrated in a way that
no other tile matches. Whether that bucket should be decomposed, or whether its regime should be pinned
deliberately rather than inferred, is a modelling question this search surfaced but did not address.

## Reproduce

```bash
source .venv/bin/activate
python scripts/run_summer_trough_grid.py --dry-run     # the 24-probe plan
python scripts/run_summer_trough_grid.py --workers 3   # ~19 min, resumable by slug
python research/param-scans/summer-trough-v2/build_grid_report.py
python scripts/tile_corr_distribution.py \
  data-official/2026-08/desktop_baseline_2026-07-28/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/mozaic_objects.legacy_desktop.2026-07-28.pkl
```

Requires the sibling mozaic checkout on `configurable-model-params` at `d781d97` or later for
`seasonality_corr_threshold`.
