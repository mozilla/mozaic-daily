# `research/headwinds/aug-post-seam-retune/` — EXPERIMENTS: matching August desktop to post-seam actuals

**These are experiments, not forecast changes.** Nothing here edits
`data-official/2026-08/adjustments/headwind.json` or any other live spec, and nothing here is
adopted. Every plot carries an "EXPERIMENT" watermark for exactly this reason.

## The shared setup

Both experiments respond to the same observation, and both are **desktop only** (mobile's `h`/`t` are
untouched). Since the August build's seam (2026-08-02, trained through 2026-08-01), actual desktop
DAU has come in above the published, headwind-adjusted forecast, and the gap is growing: **+24,589**
on the 28d MA at the seam → **+492,516** by 2026-08-16. Two different levers were tried:

| | lever varied | held fixed |
|---|---|---|
| **Experiment 1** | the `h` headwind Dec-15 anchor | model config, training window, `l`/`o` |
| **Experiment 2** | the training window (a real model refit) | `h` anchor, model config, `l`/`o` |

Common to both: IR/CN are **included as-is** (no netting of confounds; fit against the published
world-total ALL-desktop series exactly as it stands), and both are deliberately **self-contained** —
neither reconciles against `../WIN10_ANCHOR_FINDINGS.md` or any other prior headwind-magnitude
analysis.

## Files

| File | Purpose |
|---|---|
| `headwind_retune_experiment.ipynb` | Both experiments, in order. Loads the live g01 desktop parquet + July's delivered parquet unchanged, pulls a small fresh BigQuery slice for the post-seam actuals only (training rows in the parquet already cover everything before the seam — no need to rescan a year of `active_users_aggregates`), then runs Experiment 1 (closed-form anchor fit + grid + plots) and Experiment 2 (load the refit, apply the unchanged headwind, table + plot). |
| `run_refit.py` | Producer for Experiment 2's model run. Re-runs the canonical g01 desktop config at a 2026-08-17 seam. **Carries August's date-gated `l`/`o` specs forward past their `applies_to_forecast_start`** — without that the overlays silently drop out at the new seam and the refit is not comparable to the published `adj-lo` build. Read its docstring before reusing. |
| `refit_2026-08-17/` | Experiment 2's build: fresh raw BQ pull (training through 2026-08-16), `.adj-lo.` forecast parquet + sidecar, `parameters.json`, fitted `.pkl`. Not canonical. |
| `results/headwind_grid.csv` | Experiment 1's swept grid: 489 candidate anchors (10,000-DAU step) with Dec-15 value, RMSE, and distance from the stakeholder Low target. |
| `results/refit_2026-08-16_results.csv` | Experiment 2's results table. |
| `plots/desktop_headwind_retune_experiment.png` | Experiment 1 chart, canonical style, watermarked. |
| `plots/rmse_vs_headwind_anchor.png` | Experiment 1 diagnostic — RMSE vs. candidate anchor. |
| `plots/desktop_refit_through_2026-08-16.png` | Experiment 2 chart, canonical style, watermarked. |

---

## Experiment 1 — vary the headwind anchor, hold the model fixed

Asks: holding the model config, `l`/`o` overlays, and the headwind ramp's shape/start date all fixed,
what Dec-15 anchor for `h` best matches (minimizes RMSE against) the actuals observed since the seam?

- **Ramp shape and start date fixed** (linear ramp, start 2026-08-02, anchor 2026-12-15). Only the
  Dec-15 magnitude varies.
- **Fit window** = ramp start through the most recent landed actual — the 15 days of new post-seam data.
- **Objective**: plain (unweighted) RMSE. No recency weighting in this version.
- **No floor at zero** — the best-fit anchor is reported whatever sign it comes out to be.

### Why a closed form, not just a grid search

`h` is a display-layer adjustment (`src/mozaic_daily/adjustments.py`): a linear ramp added to the 28d
MA *after* mozaic. Because the ramp is linear in the anchor magnitude, RMSE against actuals is an
exact convex quadratic in the anchor with one minimum — solvable directly
(`A* = sum(f·r) / sum(f²)`, weighted least squares through the origin). The grid is swept anyway, for
visualization and audit, but it confirms the closed form rather than searching independently.

### Result — read the caveat before the number

| | anchor (Dec-15 DAU) | RMSE, fit window | Dec-15 desktop | Dec-15 − stakeholder Low |
|---|--:|--:|--:|--:|
| Live (published) | −1,315,000 | 261,177 | 48,703,443 | −336,409 |
| Best fit | **+2,954,764** | 22,913 | 52,973,207 | **+3,933,355** |

The unconstrained RMSE-best-fit anchor is a **large positive tailwind**, not a smaller headwind. Fit
window is 2026-08-02 → 2026-08-16 — **15 days, 11.1% of the 135-day ramp.** The fitted slope (`f(t)`
maxing at ~0.111 over the window) gets extrapolated roughly 9x forward to Dec-15, so a modest,
plausibly noisy early residual becomes an extreme anchor. The chart shows the consequence directly:
the "Updated" curve has to pass above July's own delivered curve and above the Stretch stakeholder
marker before the model's seasonal December decline pulls every curve back down. **The fit itself is
numerically well-behaved (see the RMSE-vs-anchor parabola) — the extremity is a property of fitting a
Dec-15-anchored ramp from 11% of its length, not a bug.**

---

## Experiment 2 — keep the original headwind, refit the model on all data

Asks: keep `h` at its published **−1,315,000** and re-run the *model* against every day of data we
now have. Only the training window varies.

| | published build | this refit |
|---|---|---|
| training through | 2026-08-01 | **2026-08-16** (+15 days) |
| seam | 2026-08-02 | **2026-08-17** |
| model config | g01 | **g01, byte-identical** (asserted in-notebook) |
| `l` / `o` | applied | applied, **same curves carried forward** |
| `h` anchor | −1,315,000 | **−1,315,000, unchanged** |

The headwind ramp start moves with the seam (135-day ramp → 120-day), following this cycle's
convention that `h` ramps from the seam forward. **Dec-15 is unaffected by that move** — the ramp is
anchored there — so it changes only the interior shape.

### Result

| build | trained through | `h` anchor | Dec-15 desktop | vs. published | vs. stakeholder Low |
|---|---|--:|--:|--:|--:|
| Prior (July delivered) | 2026-07-05 | −1,345,000 | 48,585,483 | −117,960 | −454,369 |
| Published (Aug, g01) | 2026-08-01 | −1,315,000 | 48,703,443 | — | −336,409 |
| **EXP 2 refit (same `h`)** | **2026-08-16** | −1,315,000 | **48,835,825** | **+132,382** | **−204,027** |

**15 extra days of training data are worth +132,382 DAU at Dec-15**, closing ~39% of the distance to
the stakeholder Low marker with no change to the headwind at all.

Far more modest than Experiment 1's answer, and the contrast is the useful part: here the fresher
data is absorbed by Prophet's fit to the whole training history, so its Dec-15 influence is damped.
In Experiment 1 the same data had to be explained *entirely* by one Dec-15-anchored ramp fitted from
11% of its length, so it got amplified. Same data, two very different levers.

⚠️ **The RMSE column printed alongside this table is degenerate for the refit and is not validation.**
The refit trained *through* 2026-08-16, so over the 2026-08-02 → 2026-08-16 comparison window its
curve is training rows — byte-identical to actuals by construction — giving RMSE ≈ 1. No
out-of-sample window exists for the refit yet; getting one means waiting for days it has not seen.

---

## Possible next iterations (not run)

- Recency-weighted RMSE for Experiment 1 (plain RMSE was chosen as a starting point, not a final one).
- A floor/ceiling on Experiment 1's candidate anchor, now that the extrapolation behavior is visible.
- Re-running either experiment as more days land — Experiment 1's core problem is the short elapsed
  fraction of the ramp, and Experiment 2 has no out-of-sample window yet. Both improve with time.
- Combining the levers (refit *and* re-fit the anchor) — deliberately not done, since it would
  confound the two effects that these experiments were built to separate.
