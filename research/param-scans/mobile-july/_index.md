# research/param-scans/mobile-july/

> **2026-07-29 — superseded in part by Fix A.** The seam/splice work recorded here was done against
> the pre-fix trend estimator, which inflated the first forecast day (mobile: +213,322, i.e. +7,619
> on the 28d MA). The estimator is now fixed in `src/mozaic_daily/seam_ma.py`; the `continuous_splice`
> cubic correction described below is unchanged and still shipping, but it now has a much smaller
> landing residual to correct. See `research/ma-seam-turbulence/LOG.md` § Fix A.

July 2026 mobile (`glean_mobile` DAU) parameter search. Goal: land the ALL-MOBILE,
plus-Iran, **Dec-15 28-day-MA** at **June + 400,000 = 17,911,100** with the marketing
tailwind (`m`) and headwind (`h`) both applied.

- **Baseline (June):** 17,511,100 (June delivered mobile plus-Iran curve, `display_ma`
  28d-MA at Dec-15). **Target:** 17,911,100.
- **Current center** (June-copied params): adj-m 17,825,124; adj-hm 17,797,962 →
  net **+286,862**; **~+113,138 to climb**.

## What's here

- `_archive/research/param-scans/mobile-july/mobile_july_sensitivity.ipynb` (**moved 2026-07-29** —
  it bound the frozen 2026-06 seam MA via a path list, and imports the now-archived
  `mobile_sensitivity.py`) — round-1 sensitivity notebook: one plot per knob
  (adj-hm 28d-MA horizon for −δ/center/+δ, baseline+target lines, Dec-15 net annotated),
  a knob-slope tornado, and a full decomposition of the single all-level mobile Prophet
  (training data / trend+changepoints / weekly (conditional) / yearly).
- `round1_results.csv` — per-cell adj-m/adj-hm 28d-MA, net vs June, gap to target.
- `_archive/research/param-scans/mobile-july/seam_smoothing.ipynb` (**moved 2026-07-29** — it imported
  the frozen 2026-06 seam MA, and its conclusions are specific to the pre-Fix-A estimator) — removing
  the actuals→forecast **kink** in the 28d-MA. **Terminology:**
  *seam* = actuals→forecast boundary in the raw daily (2026-06-29); *splice* = the corner point in
  the 28d-MA, 28 days later (2026-07-26). Diagnostic `scripts/seam_bridge.py::kink_score`
  (`corner_ppm` = RED flag; `drawdown` = yellow) + a splice-window corner (target < 400 ppm).
  **Fix (final): a continuous splice** in `export_canonical_curves.py::display_ma(continuous_splice=True)`
  (the default) — a **cubic correction matching both level AND slope** of the forecast-only MA at
  the splice (C1 handoff), keeping the variance-matched trailing MA. One shared path, applied to
  mobile + desktop. Mobile splice corner **1594→46 ppm** (window-max 414 = the transition's inherent
  roughness floor ~395, not the handoff); desktop **9529→968 ppm** (real July-4 holiday corner
  untouched). Dec-15 unchanged both. Plots: `plots/seam_splice_{mobile_ma,desktop}.png`.
  **Explored & dropped:** the daily-level bridge (`bridge_seam_daily`, retained in `seam_bridge.py`)
  — fixed mobile's raw-daily launch step but misrepresented desktop's real summer decline.
  **Production wiring (ship `continuous_splice=True` as the exporter default; no `.adj-` code —
  it's display-layer only) is a separate human go/no-go.**
- `results/<slug>/` — per-config scan outputs (`.gm-D.adj-m.parquet` + sidecar meta +
  `mozaic_objects.glean_mobile.2026-06-29.pkl`). Slug = `MobileModelConfig.to_slug()`.

## How it's produced (scripts, not here)

- `scripts/run_mobile_param_scan.py` — one mobile forecast for one `MobileModelConfig`
  (marketing applied in-pipeline, native Iran fill, raw-cached, `.adj-m.` stamped).
- `_archive/scripts/mobile_grid_search.py` (**moved 2026-07-29**) — round-1 OAT orchestrator (center + 2×6 perturbations)
  + central-difference slopes; writes `round1_results.csv`.
- `_archive/scripts/mobile_sensitivity.py` (**moved 2026-07-29**) — pure scorer (extraction → canonical `display_ma`
  seam MA → headwind daily-anchor convention → net vs baseline). Tested in
  `_archive/tests/test_mobile_sensitivity.py`.
- `scripts/seam_bridge.py` — seam kink diagnostic (`kink_score`) + daily-level bridge
  (`seam_level_gap`, `bridge_seam_daily`). Platform-agnostic; consumed by `seam_smoothing.ipynb`.
  Tested in `tests/test_seam_bridge.py`.

## Conventions (matched to `july_canonical_v2026-06-29.ipynb`)

- MA = variance-matched seam `display_ma`; at the Dec-15 far horizon it equals a plain
  forecast-only `rolling(28).mean()`.
- Headwind adds the **daily** ramp value on forecast dates — full −27,162 at the Dec-15
  anchor (not the ramp's 28d average). See memory `feedback_headwind_ma28_alignment`.

## Where new work goes

Round-2+ scans and their configs → new `results/<slug>/` dirs (idempotent; existing
slugs are skipped). New rounds append a `roundN_results.csv` and a notebook section or
sibling notebook. Cross-cycle mobile param methodology stays under `research/param-scans/`.
