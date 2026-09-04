# `research/param-scans/mobile-aug/`

August 2026 mobile (`glean_mobile` DAU) parameter search. **Calibration of the `p`
paid/organic build back to July's delivered mobile Dec-15.**

## The goal, and why the gap exists

| | Dec-15 2026 28d-MA, post-headwind |
|---|--:|
| **Target** (July delivered) | **17,923,869 ± 50,000** |
| Center (shipped August `p` build) | 17,601,155 |
| **Gap to close** | **+322,714** |

The gap was created deliberately on 2026-07-31 by the `m` → `p` methodology swap, not by a
data refresh. `m` handed Prophet a training series that still contained all paid growth, so
it extrapolated a **+16.12%/yr TOTAL** rate as organic and then added paid back on top;
`p` measures paid, forecasts **organic** only (**+11.60%/yr**), and stacks paid back as a
level. See `data-official/2026-08/organic/_index.md`.

The user's ruling (2026-07-31): **the target stands.** `p` is the more robust mechanism and
this is a calibration step to bring the headline back to a previously verified result.

**The cost is real and must stay visible in every report.** Lifting Dec-15 by +322,714 means
raising the fitted trend. In calendar-aligned terms the search must move mobile's Dec-15
year-over-year growth from **12.35% → 14.41%**, against a measured organic rate of 11.60%.
`mobile_scoring.py` reports `yoy_dec15_pct` on every probe so this is never implicit.

## Why July's slopes don't transfer

Under `p`, paid at Dec-15 is a fixed level — 922,250 (anchor) + 637,227 (marketing's lift)
≈ **1,559,477** — stacked post-mozaic with zero Prophet interaction. Two consequences:

- The model controls only the ~16.07M organic remainder, so the whole +322,714 must come out
  of it: **+2.01% on organic**.
- But the add-back is additive, so a curve change reaches the headline **1:1**. The retired
  `m` overlay was bidirectional and absorbed **58%** of any curve change. July's `adj-m`
  slopes therefore understate this cycle's leverage by roughly 2.4× and are not comparable.

## Scope

**Scored on Dec-15 only** (per the search brief). Seam handoff is *reported, never trained on*.

| knob | status | why |
|---|---|---|
| `prophet_changepoint_prior_scale` | **axis** | center 0.035 |
| `prophet_changepoint_range` | **axis** | center 0.75; bounded above, most non-linear in prior scans |
| `prophet_n_changepoints` | **axis** | center 25 |
| `prophet_recent_weeks` | **axis** | center 13; integer, so ±2 is a secant |
| `prophet_seasonality_prior_scale` | **axis** | center 0.1 |
| `seasonality_regime` | **tested — dud** | Held fixed per run via `--regime`; all three of `auto`, `multiplicative` and `additive` were run in full. Worth **+17,542** and **−0** respectively. On mobile the regime sets `seasonality_mode` only — growth stays volume-driven, unlike desktop — and under `auto` a tile is multiplicative iff `max(DAU) ≤ 2e6`, so the world headline is `additive` by default. See the regime section below |
| `seasonality_corr_threshold` | **not available** | `MobileModelConfig` raises on any non-zero value — mobile's regime switch is volume-driven, so there is no correlation cutoff to move. Desktop-only |
| all four holiday knobs | excluded | standing policy: strictly local effects must not move a whole-season quantity |

**`holiday_threshold` stays pinned at the shipped −0.055, not reset to the −0.032 default.**
That value is off-default, inherited from July's `grad_moderate` search — which predates the
exclusion policy. Holding it keeps the center equal to the build actually in production;
resetting it was considered and declined (2026-07-31).

## Round 1 result (2026-07-31): the five numeric knobs cannot reach the target

11 probes, center + 5 axes × ±δ. **The center reproduced 17,601,155 exactly**, confirming the
gradient sits on the shipped build.

| axis | f(−δ) | f(+δ) | d1 /unit | d2 /unit² | effect @ ±10% | nonlinearity |
|---|--:|--:|--:|--:|--:|--:|
| `changepoint_range` | 17,625,062 | 17,584,021 | −820,815 | +1.08e7 | **−61,561** | 49.5% |
| `changepoint_prior_scale` | 17,607,503 | 17,597,095 | −1,486,806 | +1.87e8 | −5,204 | 22.0% |
| `n_changepoints` | 17,595,952 | 17,602,126 | +1,029 | −470 | +2,572 | 57.1% |
| `seasonality_prior_scale` | 17,600,096 | 17,603,793 | +184,842 | +1.58e7 | +1,848 | 42.7% |
| `recent_weeks` | 17,602,898 | 17,604,491 | +398 | +1,270 | +518 | 207.2% |

**Sum of all five 10% effects: −61,826, against a gap of +322,714.** Moving every knob +10%
moves the headline the *wrong way*. Even taking each in its favourable direction, the whole
five-knob budget at ±10% is ~71,700 — **22% of the gap**. Every probe's Dec-15 lands in
17,584,021–17,625,062, a 41,041 spread; the target is 300K above the top of that range.

Single-knob linear extrapolations, all of them unusable:

| axis | center | linear target | move | verdict |
|---|--:|--:|--:|---|
| `changepoint_range` | 0.75 | 0.357 | −52.4% | in range, but see below |
| `changepoint_prior_scale` | 0.035 | **−0.182** | −620% | impossible (must be > 0) |
| `n_changepoints` | 25 | 339 | +1254% | absurd |
| `seasonality_prior_scale` | 0.1 | 1.846 | +1746% | nominally in range, 18× |
| `recent_weeks` | 13 | 823 | +6231% | absurd (~16 years) |

**`changepoint_range` is the only live lever, and it points down** — lowering it raises Dec-15.
Two things qualify that:

- **Curvature dominates at the extrapolated distance.** Over the −0.393 move to 0.357 the
  linear term is +322,745 but the quadratic term is +834,840 — larger than the target itself,
  and same-signed. The linear estimate is meaningless that far out; the true response could
  overshoot badly. Nonlinearity is already 49.5% within the probed ±10%.
- **What it means physically is probably disqualifying.** Mobile trains 2020-12-31 → 2026-07-27
  (2,034 days). `cpr` is the fraction of that window in which Prophet may place changepoints:

  | cpr | last changepoint | history left unhinged |
  |--:|---|--:|
  | 0.82 (package default) | 2025-07-26 | 366 d |
  | **0.75 (shipped)** | **2025-03-06** | **508 d** |
  | 0.60 | 2024-05-04 | 814 d |
  | 0.357 (extrapolated) | **2022-12-27** | **1,308 d** |

  At 0.357 the trend is pinned to the 2021–2022 growth regime and the last 3.6 years cannot
  bend it. That is very likely why it lifts Dec-15 — it recovers the steeper pre-paid-era slope
  — which is the same mechanism `p` was adopted to remove.

**One favourable side finding:** the seam handoff *improves* in the same direction. `cpr=0.725`
gives seam step −8,304 and slope kink −1,004, versus the center's −9,989 / −2,954. So the
promising direction does not cost handoff quality. (Reported, not scored.)

**YoY across all 11 probes spans 12.24–12.50%**, against the **14.41%** required to hit target
and the **11.60%** measured organic rate. The entire local neighbourhood is ~2pp short.

## Regime test (2026-07-31): `multiplicative` is a dud on mobile

The same 11 probes were re-run with `seasonality_regime='multiplicative'` as the center
(`python scripts/run_mobile_gradient.py --regime multiplicative`). Results in
`round1_mult_scores.csv` / `round1_mult_derivatives.csv`.

**The regime's own effect is +17,542 at Dec-15 — 5.4% of the gap.** It also slightly *worsens*
the seam handoff (step −9,989 → −10,719, kink −2,954 → −3,956).

This is the opposite of desktop, where forcing multiplicative was the largest single lever in
the summer-trough search. The reason is structural: on desktop the regime also flips growth
linear↔logistic, while **on mobile it sets `seasonality_mode` only** — growth stays
volume-driven. There was never a second mechanism for it to reach.

The gradient at the multiplicative center has essentially the same shape as at `auto`:

| axis | effect @ ±10% (auto) | effect @ ±10% (mult) |
|---|--:|--:|
| `changepoint_range` | −61,561 | −63,176 |
| `changepoint_prior_scale` | −5,204 | −5,501 |
| `n_changepoints` | +2,572 | +3,975 |
| `seasonality_prior_scale` | +1,848 | +2,908 |
| `recent_weeks` | +518 | **−332** (sign flip — noise-level) |

Sum of the five 10% effects is −62,126 under multiplicative, versus −61,826 under auto. No
change of character.

## Additive regime (2026-07-31): exactly zero — and *why* is the real finding

The same 11 probes were run a third time with `seasonality_regime='additive'`
(`--regime additive`). Results in `round1_addi_*.csv`.

**Effect at Dec-15: −0 DAU.** Every probe lands within ~3 DAU of its `auto` counterpart.

This is **not** a failed override. The config reaches the model (11 fresh slug dirs, 0 skips,
sidecars record `seasonality_regime: additive`), and at leaf level it changes plenty:

| vs `auto`, Dec-15 daily | leaves moved | Σ\|leaf move\| | net leaf move | world total move |
|---|--:|--:|--:|--:|
| `additive` | **63 of 64** | 27,552 | **+1** | **+1** |
| `multiplicative` | 60 of 64 | 11,343 | +5,439 | +5,439 |

**63 of 64 leaf tiles move by 27,552 DAU in total, and the world headline moves by 1 DAU.**
The reallocation cancels because the total is pinned.

### The mechanism, and what it means for every future mobile search

mozaic reconciles **top-down** — `mozaic/utils.py` calls
`metric_mozaics[m].reconcile_top_down(use_holidays=True)`. The metric-level Mozaic forecasts the
**aggregate** series, then rescales the 64 country × app leaves to sum to it. A leaf's model form
therefore changes the *allocation across countries*, not the total.

mozaic's mobile model is volume-gated: under `auto` a tile is multiplicative iff `max ≤ 2e6`. The
aggregate is ~16M, so **the top-level fit is already additive under `auto`**. Hence:

- forcing `additive` → top-level fit unchanged → total unchanged (−0)
- forcing `multiplicative` → top-level fit flips → total moves (+17,542 on the 28d-MA)

> **The mobile world headline is, to first order, a single Prophet fit on the aggregate organic
> series. The 64 leaf tiles set allocation only.** Any parameter whose effect varies across tiles
> largely cancels at the total; only its effect on the top-level fit survives.

That is the structural reason the whole knob set is weak here, and it should be the starting
assumption for any future mobile parameter work — including that the earlier
`seasonality_corr_threshold` question was moot twice over.

## The reachable envelope: 33 probes, all short

| | |
|---|--:|
| Best of 33 probes (`multiplicative` / `cpr=0.725`) | **17,647,560** |
| Still short of target by | **276,309** |
| Full envelope across all probes | 17,584,021 – 17,647,560 (spread 63,539) |
| YoY across all probes | 12.24% – 12.64% |
| YoY required to hit target | **14.41%** |
| Measured organic growth rate | 11.60% |

**This is a measured maximum over 33 configs, not an extrapolation.** The target sits ~276K
above the top of everything built, and the whole reachable neighbourhood is ~1.8pp of YoY
short. `plots/round1_regime_comparison.png` shows all four relevant curves against the target
band in one frame.

### Where that leaves the search

Round 1 plus the regime test answered the question they were set: **no combination of the
exposed non-holiday knobs reaches 17,923,869 from this center at defensible magnitudes.** The
remaining options all sit outside what was scoped:

1. **A large `changepoint_range` move** (≈0.36 by linear extrapolation) — but see the curvature
   and calendar tables above; its own quadratic term exceeds its linear term at that distance,
   and it works by freezing the trend in the 2021–2022 pre-paid regime.
2. **A lever outside the model config** — e.g. a mobile counterpart to the desktop `l`/`o`
   overlays, or revisiting the paid level `p` stacks back.
3. **Accept 17.6M** and carry the methodology change as a documented step-down against July.

No work has been started on any of these. This is a checkpoint for a human decision.

## OUTCOME (2026-08-03): base locked, tailwind sized

**Base locked to `auto` / `cpr = 0.725`** → Dec-15 **17,625,062** (+23,907 vs the shipped 0.75).
Promoted to `data-official/2026-08/mobile_cpr0725_2026-07-28/`; see that dir's `README.md` for why
this config and not the higher `multiplicative / cpr 0.725` (17,647,560). Chosen on seam quality and
neighbour smoothness, not on size.

**Tailwind sized at +276,000 — since ADOPTED and LIVE** (registered as adjustment code `t`, spec at
`data-official/2026-08/adjustments/tailwind.json`) → **17,901,062**, which is −22,807 from target and
**in band**. Full argument: `data-official/2026-08/tailwind/_index.md`; sizing script
`tailwind_exercise.py`; chart `plots/tailwind_276k_exercise.png`. Roughly half of it (+141,637) is
backed by the independent prototype implementation; the other half is planning judgement.

The search's own conclusion stands: **parameters cannot close this gap** — 33 probes spanned 63,539
against a 322,714 gap — so the remainder is carried by an explicit, auditable overlay rather than
laundered through the model config.

## Cross-check against the `mobile_organic_aug` prototype (2026-07-31)

`~/work/product-data-science-core/scratch/brwells/mobile_organic_aug/` reaches **17,769,954**
pre-headwind — **17,742,792 post-headwind**, still **131,077 below the target band floor**. Its
17.77M was benchmarked against a 17.8M planning target and against the superseded `m`-era
17,924,607, not against this search's band.

Parameters, organic definition, shredder correction, paid curve and app scope are all
**identical**; the entire +141,637 is architectural — 3 global tiles summed vs 64 country × app
tiles reconciled, and the consequences that follow from that (volume gating puts 96.9% of
production's tiles on the multiplicative branch and the prototype's two big tiles on additive;
Fenix organic trains on 789 days vs 2,034). ~89% of the gap sits in ordinary ex-Iran organic,
only ~11% in Iran.

Full decomposition, including a production defect it exposes (the Farsi-locale-outside-IR
population craters during Iranian shutdowns inside untreated ROW tiles) and the one cheap test
it points at (`--regime additive`, never run): **`PROTOTYPE_COMPARISON.md`**.

## What's here

- `round1_gradient.ipynb` — the analysis notebook (executed, with outputs): derivative table,
  per-axis canonical-format forecast charts, tornado, seam report, extrapolation with
  range-checks. Generated by `make_notebook.py`; regenerate rather than hand-edit.
- `round1_scores.csv` / `round1_derivatives.csv` — the `auto`-regime run.
- `round1_mult_scores.csv` / `round1_mult_derivatives.csv` — the `multiplicative` re-run.
- `round1_addi_scores.csv` / `round1_addi_derivatives.csv` — the `additive` re-run.
- `round1_run.log`, `round1_mult_run.log`, `round1_addi_run.log` — driver output for each.
- `PROTOTYPE_COMPARISON.md` — why the `mobile_organic_aug` prototype reads 17.77M.
- `results/<slug>/` — per-probe outputs (`.gm-D.adj-p.parquet` + sidecar + `.pkl`).
  Slug = `MobileModelConfig.to_slug()`. Gitignored; ~838 MB pickle each.
- `plots/` — per-axis canonical-format forecast charts.

## How it's produced

- `scripts/run_mobile_param_scan.py` — one mobile forecast for one `MobileModelConfig`.
  All seven non-holiday knobs are flags as of 2026-07-31; the three seasonality ones were
  added for this search.
- `scripts/mobile_scoring.py` — the pure scorer. Reproduces the published 17,601,155 exactly,
  which is what licenses comparing probes against it. Tested in `tests/test_mobile_scoring.py`.
- `scripts/run_mobile_gradient.py` — the round-1 driver (center + 5×2, parallel, idempotent,
  slug-collision guarded).

July's equivalents (`mobile_grid_search.py`, `mobile_sensitivity.py`) are in `_archive/scripts/`:
both were July-scoped and the driver had been unrunnable since 2026-07-29.

## Reading the derivatives

The three-point stencil gives `d1 ≈ (f(+h) − f(−h)) / 2h` and
`d2 ≈ (f(+h) − 2f(0) + f(−h)) / h²`. **`value_to_close_gap` in the CSV is a linear
single-knob extrapolation and is a round-2 starting point, not a prediction.** `d2` says how
fast that estimate degrades, and cross-parameter non-linearity — a consistent finding in
every prior search on this codebase — is not captured by it at all.

## Where new work goes

Round-2+ configs → new `results/<slug>/` dirs (idempotent; existing slugs are skipped), plus
a `roundN_*.csv` pair and a notebook section. Anything that outlives this cycle (mobile param
methodology, cross-cycle comparisons) stays under `research/param-scans/`; the winning build
itself goes to `data-official/2026-08/`.
