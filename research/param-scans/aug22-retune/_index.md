# `research/param-scans/aug22-retune/` — near-horizon (Aug-2026 trough) desktop retune

Desktop-only parameter search that targets the **28-day trailing MA of world-headline
`legacy_desktop` DAU at the summer trough (2026-08-22)**, measured post-headwind (display layer).
Bullseye **45.06M ±0.1M**, with Dec-15 held near the locked July value (48.585M, ±10k tolerance).

This is the *near-horizon* counterpart to the Dec-15 searches at the parent level
(`../desktop_gradient_round{1..4}.ipynb`). Same anchor **2026-07-06**, same cached raw data,
search center = the **locked July config** (cps 0.08983, cpr 0.65, recent 13, ncp 25, sps 0.00825)
rather than package defaults.

## Outcome (read this first)

**The parametric search failed to hit both bands, and that negative result is the deliverable.**
No exposed parameter combination reaches Aug 45.06M while holding Dec-15 within tolerance:

| lever | Aug effect | Dec effect | verdict |
|---|---|---|---|
| `seasonality_prior_scale` (sps) | ±2.0M | ±2.9M | symmetric 1:1.4 trade — kills Dec |
| `seasonality_regime=multiplicative` | **+1.29M** | +0.19M | the shape lever (~6.7:1 asymmetry), but plateaus ~44.54M |
| trend knobs (cps / cpr / ncp / recent) | ±100–150K | small | `cpr` is the only live one; cps/recent inert |

Best sampled point (28-config Latin-hypercube, `sampling/`) = **s01** (multiplicative, cps 0.1849,
cpr 0.734, recent 17, ncp 35): Aug 44.675M (**−0.385M short**), Dec-15 −60k. Nothing closes the gap.

**Recommendation carried forward:** stay on the production `auto` regime + locked params and add a
**summer-trough overlay** — a new bidirectional adjustment code in the `l`/`o`/`m` family (subtract
from training pre-mozaic, add back to the forecast) that lifts the Aug 28d-MA to target and tapers
to ~0 by Nov/Dec so the winter peak is held by construction. See `round3/FINDINGS.md`.

## What's here

| Path | Purpose |
|---|---|
| `round1/FINDINGS.md` | One-at-a-time ±δ around the locked center → local slopes. Discovers sps as the only strong knob. |
| `round2/FINDINGS.md` | `seasonality_regime` sweep (auto / additive / multiplicative) × sps. Finds the multiplicative asymmetry. |
| `round3/FINDINGS.md` | Trend knobs *under* multiplicative — establishes the parametric ceiling + the overlay recommendation. |
| `sampling/sampling_scores.csv` | 28-config Latin-hypercube over the multiplicative frontier; no `FINDINGS.md` — s01 is the winner and the conclusion is in `round3`. |
| `desktop_bestfit_vs_july.ipynb` | Plots the s01 best-fit 28d-MA curve against the locked July forecast and actuals. |
| `plots/desktop_bestfit_vs_july.png` | That plot. |
| `bestfit_28tma_curve.csv` | s01's 28d-MA curve (global + ex-CN/IR, pre/post headwind), exported by `export_bestfit_curve.py`. |
| `parameter_table.html`, `build_report.py`, `make_notebook.py` | Report/notebook generators for the above. |

## Present vs Archived

- **Present on this branch (slimmed):** `_index.md`, the three `FINDINGS.md`, the four
  `*_scores.csv`, `bestfit_28tma_curve.csv`, the notebook + plot, `parameter_table.html`, and the
  generator scripts. The search's method, evidence tables, and conclusion are fully legible from
  these alone.
- **On the `july-forecast` branch only:** the ~197 per-probe sidecars and logs
  (`round{1,2,3}/*/run.log`, `round*.log`, `sampling.log`, `_rawcache/fetch.log`, and every
  `round*/*/*/parameters.json` + `*.meta.json`). That branch is the permanent full record — recover
  a specific config's exact settings from there, e.g.
  `git show july-forecast:research/param-scans/aug22-retune/round2/mult__sps005/.../parameters.json`.
- **Archived to GCS (`gs://…/july-2026/`), removed from disk:** the ~40G of per-config forecast
  `*.parquet`, `mozaic_objects.*.pkl`, and `_rawcache/` (633M). Regenerable but expensive — each
  round is hours of Prophet fits, so pull from GCS rather than re-running.

## Regenerating

Requires the raw cache (or a fresh BQ pull) plus the two mozaic knobs this search introduced.

```bash
source .venv/bin/activate

python scripts/run_aug_trough_gradient.py --round 1 \
    --raw-cache-dir research/param-scans/aug22-retune/_rawcache \
    --parallel 4

# preview a round's probe list without running it:
python scripts/run_aug_trough_gradient.py --round 3 --dry-run
```

Every probe is scored inline by `scripts/score_near_horizon.py` (Global + ex-CN/IR, pre/post
headwind, plus the Dec-15 side-effect) straight into the round's `*_scores.csv` — no separate
notebook pass needed to read the slopes.

## Dependency: mozaic package knobs

`seasonality_prior_scale` and `seasonality_regime` are **not in the released mozaic package**. They
come from the sibling checkout `mozaic-forecasting-official` on branch `configurable-model-params`
(commits `126fe14` "Expose seasonality_prior_scale…" and `6f02912` "Add seasonality_regime enum…").
`scripts/run_param_scan.py` and `run_aug_trough_gradient.py` forward them via
`DesktopModelConfig`. **This search is not reproducible against a package without those commits.**

## Related

- Parent Dec-15 searches: `../desktop_gradient_round{1..4}.ipynb`, `../_index.md`
- The summer-slump seasonal shape this search was trying to reproduce: `../../summer-slump/`
- Overlay machinery the recommendation would reuse: `src/mozaic_daily/adjustments.py`, `data-official/adjustment_codes.yaml`
