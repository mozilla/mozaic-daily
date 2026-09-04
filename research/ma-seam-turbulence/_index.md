# `research/ma-seam-turbulence/` — early-horizon 28dMA turbulence investigation

**RESOLVED (2026-07-29) — Fix A shipped.** The second seam artifact (`reconstruct_matched_daily`
deseasonalizing with a 7-day *centered* mean of the raw forecast, so `min_periods=4` let it
degenerate to a weekday-only forward window at the seam, stepping the published desktop curve
**+102,595**) is fixed by **deseasonalizing before averaging**. Aug desktop display distortion
211,480 → **116 DAU**; Dec-15 byte-identical on both platforms. Full record: **`LOG.md` § Fix A**.

The fix lives in **`src/mozaic_daily/seam_ma.py`**, a new package home. `data-official/2026-06/export_canonical_curves.py`
is deliberately **untouched** so June's and July's delivered curves cannot move; code still bound to
it was moved to `_archive/`. Tests: `tests/test_seam_ma.py` (20, incl. a canary that fails if the
suite ever stops catching this defect class).

**Read these two, in order:**

- **`seam_step_diagnosis.ipynb`** — the *why*. A rerunnable, plot-per-step walkthrough from the
  published August chart back to the source arrays, built cold without reading the handoff below, and
  independently reproducing the same mechanism and the same +102,595. Five rungs: rules out a
  data-source mismatch, shows the model is continuous, isolates the weekday-only trend window,
  converts the one-day error into the MA step, closes the waterfall to <1 DAU, then falsifies against
  253 backtested seam dates (the error's sign tracks the seam's weekday, monotone in how many weekend
  days land in the forward-4 window).
- **`data-official/2026-08/seam_fix_before_after.ipynb`** — the *verification*. Published vs fixed
  curves on both platforms, and an assertion that Dec-15 did not move.

**Two negative results worth not re-deriving** (both in `LOG.md` § Fix A): variant **A3** looked
better on the 253-seam identity backtest yet lost on the real builds, because that backtest is
structurally blind to A3's only failure mode — a cancellation-free metric is not automatically a
relevant one. And **H6**, which §H6 below recommends taking as "free", is mildly *harmful* on top of
Fix A and was deliberately left in place.

`HANDOFF_recon_edge_bias.md` is retained as the historical brief; its §7 acceptance criteria were
**retired by decision**, not met — see `LOG.md` § Fix A.


Cross-month, topic-anchored mechanism work (per the repo hybrid rule): diagnoses why the
per-country desktop forecast curves, plotted as 28-day moving averages, oscillate for ~1
month after the forecast-start date and then smooth out, and remediates it for the
stakeholder-facing curves.

## What's here
- **`LOG.md`** — append-only hypothesis ledger (confirmed + refuted). The audit trail; read
  this first. Chronological: the Phase-1 verdict is mid-file, **§ Fix A is at the end** and is current.
- **`diagnose_seam.py`** — Phase-1 diagnostic. Reads only the canonical 2026-05-26 desktop
  parquet (no BQ, no model re-run). Emits AR/US decisive figures and a per-country seam-metrics
  table (`plots/decisive_*.png`, `plots/per_country_metrics.csv`).
- **`weekly_amplitude.py`** — clinching measurement: weekly seasonality amplitude of recent
  actuals vs early forecast per country (`plots/weekly_amplitude.csv`); explains the
  day-of-week seam-step red herring.
- **`backtest_seam.py`** — Phase-2B decision gate. Out-of-sample April backtest (parquet-only,
  no BQ): scores the OLD straight bridge vs the NEW variance-matched transition against the
  realized April 28dMA (June parquet training rows), via bias-removed (shape) MAE. The gate is
  decided on the **global ALL-level** transition; per-country is a printed diagnostic. Writes
  `plots/april_backtest.csv` + `plots/april_backtest_{platform}_{ALL,AR,US,...}.png`.
- **`build_report.py`** — builds `report.html` (self-contained, base64 images) from the log +
  saved plots, incl. the April-backtest evidence panel. *(added in Phase 3, extended in 2B)*
- **`report.html`** — boss-facing diagnostic: problem, what it is NOT, root cause, the fix with
  before/after examples, why it won't recur. *(added in Phase 3)*
- **`plots/`** — saved figures and CSV tables, including the six `step_rung*.png` figures from
  `seam_step_diagnosis.ipynb`.
- **`extracts/`** — cached BQ pulls so the diagnosis is rerunnable without re-querying.
  `desktop_actuals_2026-06-01_2026-07-27.csv` is force-added to git (it is tiny, and regenerating it
  costs a ~93 GB scan).
- **`plots.zip`** — archived Phase-1 figures.

### Phase-3 / Fix A harness (the trend-estimator defect)

All read-only: no BQ, no model re-runs, no spec edits.

- **`recon_variants.py`** — the estimator variants behind a common signature (`current`, `forward7`,
  `concat`) plus `patched_reconstructor` to swap one in, and a fidelity assertion that `current`
  reproduces the shipped function exactly.
- **`diagnose_recon_edge_bias.py`** — the per-day reconstructed-vs-raw table and the edge-bias measurement.
- **`eval_recon_edge_fix.py`** — scores a variant against the handoff's original seven criteria.
- **`diagnose_splice_metric.py`** — decomposes the day-27 `visible` step into landing residual + slope,
  which is how criterion 3 was shown to be a cancellation.
- **`eval_splice_correction_load.py`** — how much work the cubic splice correction is doing.
- **`backtest_recon_variants.py`** — realized backtest across several June-cycle seams.
- **`check_delivered_numbers.py`** — asserts June/July delivered values still reproduce under a variant.
- **`eval_deseason_variant.py`** — scores the **deseasonalize-before-averaging** variants (A1/A2) and
  introduces the **identity backtest**: on all-actuals input the transition must be a no-op, so any
  deviation from the plain rolling mean is pure estimator error against known ground truth.
- **`plan_probe_fix_a.py`** — the implementation-planning probe: which existing tests a variant breaks,
  A1 vs A2 vs A3, far-horizon invariance across all six delivered builds, and transition-window shift.
- **`verify_fix.py`** — Dec-15 delta check for a candidate fix.

## What isn't here
- **The live fix**, which is `src/mozaic_daily/seam_ma.py` (`display_ma` +
  `reconstruct_matched_daily`), test-locked by `tests/test_seam_ma.py`. The **v1** fix shipped in
  `data-official/2026-06/export_canonical_curves.py`, which is now frozen and must not be edited —
  cycles through 2026-07 import it, and code still bound to it lives in `_archive/`.
- This directory is diagnosis + gating + reporting only.

## Remediation, v1 (Phase 2B — superseded in part by Fix A)
The display-side fix is a **variance-matched seam transition** spliced to the clean
forecast-only MA at +27d: the forecast's first 27 daily values are rebuilt to carry the recent
actuals' weekly amplitude, so the trailing 28d window cancels the weekly cycle and the transition
rides the forecast's true trend (curvature and all). This replaced the Phase-2A straight/linear
bridge. Gated on the **global ALL-level** April backtest (desktop shape MAE −70%). Per-country
shape regressions (e.g. desktop IN) and a small per-country splice kink are accepted, documented
v1 limitations — this is a bandaid for the global curve, not a per-country fix. See `LOG.md`
Phase 2B.

## Verdict, Phase 1 (see LOG.md)

Still accurate as the *v1* diagnosis; **Fix A later found a second, independent defect in the same
function** — the trend estimator's day-of-week-unbalanced window at the seam. See the banner above.

Primarily an **MA-seam artifact**: the plotted forecast 28dMA blends trailing raw actuals with
forecast for its first 27 points, and a weekly-pattern discontinuity at the seam (dominantly a
**damped forecast weekly amplitude** vs higher actuals amplitude for high-DOW-swing countries)
prevents the 4-week MA from cancelling the weekly cycle during the transition. Steady-state MA
and Dec-15 are unaffected; US/CA are correctly smooth. Fix is display-side.

Related: `csv-vs-actuals/`, `april-vs-june-mechanism/` (**archived to GCS, not on disk**), and the memory notes
`[[project_actuals_vs_april_overlap]]`, `[[project_april_vs_june_mechanism]]`.
