# `research/param-scans/aug25-gap/` — narrowing the Aug-25 August-vs-July gap

## Read this first: the direction here is DOWN

This search moves the desktop **trough down**. Its sibling directories under
`research/param-scans/` (`aug22-retune/`, `summer-trough-v2/`) were tuned to move the trough **up**,
toward a 45M–46M band. **Their targets, gradients and sampled points do not transfer here** and were
deliberately not read while designing this search. If you arrive from one of those directories, drop
their objective at the door.

The one shared tool, `scripts/score_near_horizon.py`, still prints a `target band : 45M-46M` line
inherited from that earlier objective. **Ignore it.** The only Aug-25 criterion here is the band in
`PLAN.md`.

## What this is

The August desktop forecast sits +1,961,825 above July's delivered curve at the scored trough date.
The task is to close **10%** of that — pull Aug-25 down by **196,183** — using Prophet parameters
only, while Dec-15 stays within ±50,000.

| | value |
|---|--:|
| Aug-25 baseline (`data-official/2026-08/desktop_locked/`) | 45,223,249 |
| **Aug-25 target** | **45,027,066** ± 25,000 |
| Dec-15 baseline | 48,703,960 ± 50,000 (rank by smallest drift) |
| seam-kink guardrail (model-only) | −9,554 DAU/day, must not materially worsen |

`PLAN.md` holds the full contract: locked target, frozen levers, round structure, and the promotion
gate.

## Outcome — ADOPTED 2026-07-30 as the canonical August desktop config (`g01`)

**`cps 0.1649, cpr 0.814, ncp 40, recent 17, sps 0.00825` (multiplicative) is now canonical**, in
`data-official/2026-08/desktop_locked/`. The s01 config it replaced is preserved as a **revert
target** at `data-official/2026-08/desktop_s01_REVERT_2026-07-29/`.

Adopted with a paired **+25,000 headwind attenuation** (−1,245,000 → −1,220,000) absorbing most of
the config's −31,357 Dec-15 drop. The two changes revert as one unit.

Published result: Aug-25 **45,041,389**, Dec-15 **48,697,603**, ALL Dec-15 **66,622,210**, seam kink
−16,549 (1.73×). Config-isolated the retune is **−186,860 at Aug-25 (9.52% of the gap)** for −31,357
at Dec-15. The canonical notebook's ledger closes to **−0** and its config lock asserts g01.

Known and accepted: g01 is an **isolated optimum** (all seven measured one-step neighbours are
52,092–165,860 shallower) and carries a **1.73×** seam-kink regression, against a ~1.44× floor that
no feasible config avoided.

## Round-6 outcome (superseded by adoption, kept as the record)

**Round 6's 243-cell full factorial found a config that HITS the target**, overturning the
five-round conclusion recorded below. `cps 0.1649, cpr 0.814, ncp 40, recent 17, sps 0.00825`
reaches Aug-25 **45,036,389** (+9,323 vs target, inside ±25,000) with Dec-15 −31,357 and kink
−16,549 — **95.2%** of the required move.

**It is a spike, not a basin**: all ten one-step neighbours are 52,092–165,860 higher, and only
1 of 243 cells lands in the band. Not recommended for adoption without a fine local sweep.

Why five one-at-a-time rounds missed it: **`ncp` carries 40.6% of Aug-25 variance but Round 1's
±δ probe at cpr=0.734 measured it as inert**. Its potency is conditional on `cpr` — the `cpr:ncp`
interaction is 18.5% of Aug-25 variance and 21.6% of Dec-15 variance. Interactions are 27% of
Aug-25 and **55%** of Dec-15 variance overall. Full detail in `LOG.md` § Round 6.

The section below records the pre-Round-6 state and is retained as the honest record of what five
rounds of one-at-a-time probing concluded — and of why that method was insufficient here.

## Outcome after five rounds (2026-07-30, SUPERSEDED) — target not reachable

**42 configs over five rounds. 0 hit the target. 35 held Dec-15 in budget — Dec-15 never bound.**

Best achievable: **`cpr = 0.784`**, s01 elsewhere → Aug-25 **45,195,814**, i.e. **−27,436** or
**14.0%** of the requested move, at −9,266 Dec-15 and a kink 986 worse. Staged as a candidate at
`data-official/2026-08/desktop_candidate_aug25/` — **candidate only; `desktop_locked/` is untouched
and still canonical.**

Why it stops there: **Aug-25 is controlled almost entirely by the additive/multiplicative seasonality
decision, and that decision is effectively binary.** Its only operating points are ~45,223,000 and
~43,868,000; the target sits in the 1.32M gap between them. Three separate levers were each found to
be a cliff rather than a ramp (`seasonality_regime`, then `seasonality_corr_threshold`, then
`changepoint_range` past 0.784). Everything else moves Aug-25 by 1–2% of what is needed, and the
knobs **do not stack** — the measured 3-knob combination is worse than the best single knob.

Figures: `plots/final_candidate.png` (candidate + the whole reachable set),
`plots/round2_blend_gap.png`, `plots/corr_dial_full.png`, `plots/round1_gradient.png`.
Full round-by-round evidence: `LOG.md`. Ranked results: `leaderboard.csv`.

## What's here

| path | what |
|---|---|
| `PLAN.md` | The contract. Target, constraints, frozen levers, round design, promotion gate. |
| `LOG.md` | Append-only round log. **Dead ends are kept**, not pruned. |
| `run_gradient_round1.sh` | Round-1 driver: ±δ on each of the 5 primary knobs about s01, 3 concurrent. |
| `run_blend_round2.py` · `run_corr_round3.py` · `run_dense_round4.py` · `run_cpr_round5.py` | Round drivers 2–5. |
| `run_grid_round6.py` · `run_finesweep_round7.py` | The 243-cell factorial and the 108-cell edge extension. Both pre-check ADC and abort the whole run on auth failure. |
| `score_gradient.py` | Scores every run + the s01 center; emits the central-difference gradient, curvature and efficiency tables. |
| `analyze_grid.py` | Exact ANOVA decomposition of the 3^5 grid over all 31 effects; asserts they sum to 100%. |
| `select_candidate.py` | **The adoption rule**: Aug-25 ±75,000 hard, Dec-15 ±50,000 hard (prefer ≤40,000), minimise seam-kink increase. Supersedes `leaderboard.py`, which is kept as the record of how rounds 1–6 were judged. |
| `plot_candidates.py` | Standalone comparison of the candidates vs the locked build, July and actuals. |
| `runs/<slug>/` | One candidate build per dir (parquet + sidecar + `parameters.json` + symlinked raw cache). |
| `logs/` | Per-run stdout and the driver log. |
| `scores.csv`, `gradients.csv` | Accumulated scores and derived gradients (written by `score_gradient.py --csv`). |
| `plots/` | Figures. |

## What's NOT here

- **No canonical artifact — but one of these runs BECAME canonical.** Every build here is a research
  artifact; the published forecast lives in `data-official/2026-08/desktop_locked/`. On 2026-07-30 the
  g01 run was *copied* there (not moved), so its `runs/` copy remains and is now frozen — do not re-run
  or delete it. During the search itself `desktop_locked/` was treated as read-only, including as a
  symlink target.
- **No overlay or headwind work** *during the search*. The LOL ceiling (200K) and the Win10 headwind
  (then −1,245,000, seam-anchored) were frozen inputs, not search variables. The +25,000 headwind
  attenuation applied at adoption was a separate decision taken with the swap, not a search result.
- **No holiday-parameter tuning.** All four holiday knobs stay at package defaults, and
  `seasonality_regime` stays `multiplicative`. Standing policy: strictly local effects must never be
  used to move a whole-season quantity.
- **No mobile.** Desktop only; mobile is not re-run.
- **No BigQuery queries.** Every run symlinks the shared raw pull from
  `data-official/2026-08/desktop_baseline_2026-07-28/cps0.08983_.../`, so all candidates train on
  byte-identical input and every difference is attributable to parameters.

## Where new files go

- A new **round driver** → here, as `run_<purpose>.sh` or `.py`. **Never `tmp/`.**
- A new **candidate build** → `runs/<slug>/`, created by `scripts/run_param_scan.py`.
- **Plots** → `plots/`. Never `tmp/`.
- **Findings** → appended to `LOG.md`, one entry per round, dead ends retained.
- The **approved winner** → both gates have now been passed for g01: staged as a candidate, then
  promoted to `data-official/2026-08/desktop_locked/` on 2026-07-30 with s01 preserved as a revert
  target. Any *further* promotion needs the same two explicit approvals — see `PLAN.md` § Promotion.
