# Aug-25 gap-narrowing parameter search — PLAN

**Status:** **CLOSED 2026-07-30 after five rounds — target not reachable on Prophet parameters.**
Best achievable is 14.0% of the requested move (`cpr=0.784`), staged as a candidate at
`data-official/2026-08/desktop_candidate_aug25/`. Canonical `desktop_locked/` untouched.
See `_index.md` § Outcome and `LOG.md`. The round plan below is retained as the record of what was
designed and why; rounds 3–5 departed from it as the results dictated, each documented in `LOG.md`.
**Opened:** 2026-07-30. **Branch:** `august-forecast`.

---

## Context

The August desktop forecast sits well above July's delivered curve across the whole near horizon.
At the scored trough date the two are **1,961,825 DAU apart**:

| | Aug-25 (28d-MA, post-headwind) |
|---|--:|
| August current (`desktop_locked/`, s01, LOL 200K, hw −1,245,000) | 45,223,249 |
| July delivered (`desktop_current_july`) | 43,261,424 |
| **gap** | **+1,961,825** |

The ask is to close **10% of that gap** — pull Aug-25 down by **196,183** — while leaving the
December headline essentially where it is.

This is a *candidate* exercise. The current curve is liked and must survive intact: every artifact
produced here lives in a side folder, and nothing becomes canonical without an explicit approval
step.

---

## Target and constraints (locked with the user, 2026-07-30)

| item | value |
|---|---|
| **Scored date** | **2026-08-25**, fixed. Score this date regardless of where the candidate's own minimum lands. |
| **Aug-25 target** | **45,027,066** (= 45,223,249 − 196,183) |
| **Aug-25 tolerance** | **±25,000** → accept band **45,002,066 – 45,052,066** |
| **Dec-15 baseline** | 48,703,960 |
| **Dec-15 budget** | **±50,000 hard cap** (48,653,960 – 48,753,960). Among candidates that hit Aug-25, **rank by smallest \|Δ Dec-15\|**. |
| **Measurement basis** | Desktop, `country=ALL`, `segment={"os":"ALL"}`, 28-day MA via `display_ma` (Fix A), **post-headwind** |
| **Guardrail** | **Seam continuity / kink.** Baseline model-only kink at the 2026-07-28 seam is **−9,554 DAU/day** (−18,447 including the headwind's non-parameter-addressable −8,893 contribution). A candidate must not materially worsen this. |

### Levers

**In scope — Prophet continuous knobs only, perturbing s01 as the center:**

| knob | s01 value |
|---|--:|
| `--changepoint-prior-scale` | 0.1849 |
| `--changepoint-range` | 0.734 |
| `--n-changepoints` | 35 |
| `--recent-weeks` | 17 |
| `--seasonality-prior-scale` | 0.00825 |

**Frozen — do not move:**

- `seasonality_regime` stays **`multiplicative`**; `seasonality_corr_threshold` stays 0.0.
- All four holiday knobs stay at package defaults (`-0.032 / 5 / 3 / -0.6`) — standing policy that
  strictly local effects must never be used to move a whole-season quantity.
- **LOL ceiling (200K)** and **headwind anchor (−1,245,000)** and its seam start date. The headwind is
  the user's post-hoc lever for absorbing residual Dec-15 drift; it is not a search variable.
- Mobile. Untouched, not re-run.
- `o` and `m` specs. Untouched.
- Everything under `data-official/2026-08/desktop_locked/` and `desktop_baseline_2026-07-28/` —
  **read-only**, including as symlink targets. (`desktop_superseded_lol180k_2026-07-28/` was listed here
  too until it was deleted on 2026-07-30 with the non-200K LOL curves; its surviving twin under
  `research/param-scans/summer-trough-v2/s01_gradient/cps0.1849_…_regimemultiplicative/` is read-only on
  the same grounds.)

---

## Context-only inputs (goals NOT adopted)

These were read for orientation. **Their objectives are explicitly not this search's objectives:**

- `data-official/2026-08/_index.md` — describes a cycle whose near-horizon work aimed to **raise** the
  trough. This search moves it **down**. Do not carry over its targets.
- `scripts/score_near_horizon.py` prints a `target band : 45M-46M` line. That band belongs to the
  earlier raise-the-trough objective. **Ignore that line entirely** — the only Aug-25 criterion here is
  the 45,027,066 ± 25,000 band above. (They happen to overlap; that is coincidence, not agreement.)
- `research/param-scans/aug22-retune/` and `research/param-scans/summer-trough-v2/` — **deliberately not
  read.** Both were tuned toward raising the trough. Their gradients are not reusable here without
  importing their direction, and their sampled points were selected under a different objective.
- The s01 retune's headline (`trough +1,359,887 for +5,642 at Dec-15`) is a **multi-knob** move — cps,
  cpr, ncp, recent_weeks and regime all changed together. It **cannot** be used to calibrate a
  single-knob step size. It is quoted here only as evidence that the space has ample Aug-25-per-Dec-15
  leverage, not as a gradient.

---

## Tooling — reuse, don't rebuild

| need | existing tool | notes |
|---|---|---|
| produce a candidate forecast | `scripts/run_param_scan.py` | The only reproducible producer for a locked desktop config. `run_main.py` has no parameter flags. |
| score Aug-25 + Dec-15 + seam kink | `scripts/score_near_horizon.py --target-date 2026-08-25` | **Verified 2026-07-30**: reproduces the canonical build at 45,223,249 / 48,703,960 exactly, and reports both seam-kink figures. Already imports the Fix-A `display_ma` from `mozaic_daily.seam_ma`. |
| load artifacts safely | `mozaic_daily.adjustments.load_forecast(..., require_state=["l","o"])` | Guards against the date-gate trap where a mis-gated run silently emits `.raw.` with no overlays. |

**No new production code is required.** The one new artifact is a thin round-driver + comparison
notebook (below), which lives in this research directory, not in `tmp/`.

The throwaway probe `tmp/measure_aug15_gap.py` is superseded by `score_near_horizon.py` and will be
deleted.

---

## Folder layout

```
research/param-scans/aug25-gap/          # all search work
  PLAN.md                                # this file
  _index.md                              # what's here, what isn't, where new files go
  LOG.md                                 # append-only round log; dead ends kept
  runs/<slug>/                           # one dir per candidate build
    mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet (+ sidecar)
    parameters.json
    mozaic_parts.raw.legacy.desktop.DAU.parquet   -> symlink to the shared raw cache
  scores.csv                             # accumulated (slug, aug25, dec15, kink, verdict)
  aug25_gap_candidates.ipynb             # comparison notebook + canonical-format plots
  plots/                                 # all figures

data-official/2026-08/desktop_candidate_aug25/   # ONLY the approved winner, copied here later
```

The shared raw BQ pull is reused by symlink from
`data-official/2026-08/desktop_baseline_2026-07-28/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/mozaic_parts.raw.legacy.desktop.DAU.parquet`
— the same pattern `desktop_locked/` uses. **No BigQuery re-query happens in this search**, so every
round is on byte-identical input data and differences are attributable to parameters alone.

---

## Round structure

Expect **3–4 rounds**. **The project is not scoped to end at a fixed round count** — each round ends
with a report and a stop, and the user decides whether to continue, redirect, or accept.

**Hard rule between rounds:** report results and stop. Never start the next round autonomously.

### Round 1 — full ±δ gradient and curvature about s01 (10 runs)

A single-knob probe was considered and **rejected**: the required move is large enough that one knob is
unlikely to deliver it inside the Dec-15 budget, and knowing *which* knob has the best
Aug-25-per-Dec-15 efficiency is worth more than knowing one knob's slope precisely. So Round 1 samples
**every primary knob symmetrically about s01**.

Central differences about the s01 point (already scored, so it costs no run):

| knob | −δ | **s01** | +δ | δ | δ as % |
|---|--:|--:|--:|--:|--:|
| `changepoint_prior_scale` | 0.1649 | **0.1849** | 0.2049 | 0.02 | ±10.8% |
| `changepoint_range` | 0.684 | **0.734** | 0.784 | 0.05 | ±6.8% |
| `n_changepoints` | 30 | **35** | 40 | 5 | ±14.3% |
| `prophet_recent_weeks` | 14 | **17** | 20 | 3 | ±17.6% |
| `seasonality_prior_scale` | 0.0065 | **0.00825** | 0.01 | 0.00175 | ±21.2% |

**10 runs**, one knob moved per run, everything else at s01. `n_changepoints` and `recent_weeks` are
integers, so their δ is integral and symmetric — required for a valid central difference.

For each knob and each of the two scored dates this yields:

- **first derivative** — `(f(+δ) − f(−δ)) / 2δ`, the local gradient
- **second derivative** — `(f(+δ) − 2·f(s01) + f(−δ)) / δ²`, the curvature

Curvature is the reason for sampling both sides rather than one. This repo has already seen 2.4×
curvature on a knob in this family, so a one-sided slope extrapolated to a 196,183 move would be
unreliable. The second derivative also tells us directly whether a linear solve in Round 2 is
legitimate or whether we need to bracket-and-bisect.

Deltas are linear, but results will be reported as **elasticity** (% change in DAU per % change in
knob) as well as raw slope, so the five knobs are rankable against each other despite living on
different scales.

Command template (one per run; `--raw-cache-dir` does the symlinking, so **no BigQuery query is
issued**):

```bash
source .venv/bin/activate
python scripts/run_param_scan.py --forecast-start-date 2026-07-28 \
    --results-dir research/param-scans/aug25-gap/runs \
    --raw-cache-dir data-official/2026-08/desktop_baseline_2026-07-28/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825 \
    --changepoint-prior-scale 0.1649 \
    --changepoint-range 0.734 --n-changepoints 35 --recent-weeks 17 \
    --seasonality-prior-scale 0.00825 --seasonality-regime multiplicative \
    --holiday-threshold -0.032 --holiday-max-radius 5 \
    --holiday-min-radius 3 --holiday-effect-floor -0.6
```

Driven by `run_gradient_round1.sh` in this directory (**not** `tmp/`), **3 runs concurrent** on 14
cores / 48 GB. Per-run wall-clock is **not yet known**; it is logged per run and reported.

**Round 1 deliverable:** an 11-row table (`s01` + 10) of `(Aug-25, Δ vs target, Dec-15, Δ vs baseline,
seam kink)`; a per-knob gradient/curvature/elasticity table at both dates; the **Aug-25-per-Dec-15
efficiency ratio per knob** ranked against the required ~4:1; a canonical-format forecast plot; and a
statement of which single knob or knob-pair the gradient says can reach the target. Then stop.

### Round 2 — solve and confirm (1–3 runs)

Driven entirely by Round 1's gradient table:

- **If one knob clears ~4:1 with room** — solve for the value landing Aug-25 on 45,027,066 using the
  fitted local quadratic, run **one** confirmation.
- **If no single knob clears 4:1** — combine the two highest-efficiency knobs, using the gradients to
  pick a direction in the 2-D plane that maximises Aug-25 movement per unit Dec-15 movement, and run
  one or two points along it. Cross-terms are unmeasured by a one-at-a-time design, so a combined move
  must be confirmed by an actual run, never predicted by summing two single-knob deltas.
- **If curvature is large** — abandon the linear solve and bracket-and-bisect instead.

Which of these applies is a **user decision point**, not an automatic branch. Round 1's report will
state which the data supports and ask.

### Round 3 — confirm and characterise

Confirmation run at the chosen config, plus the full deliverable package:

- Scored table against all criteria including the seam-kink guardrail.
- Canonical-format desktop plot (candidate vs current vs July vs actuals from Jan 1).
- The intermediate points (Sep-15, Oct-15, Nov-15, Dec-31) so the shape between trough and Dec-15 is
  visible even though only two dates are scored.
- A single-knob attribution statement: exactly which parameters differ from s01 and by how much.

### Round 4 — reserve

Held for a fine interpolation if Round 3 lands outside ±25,000, or for a user-directed redirect.

---

## Promotion to canonical — gated, never automatic

Nothing moves out of `research/` without an explicit approval. On approval:

1. Copy the winning run dir to `data-official/2026-08/desktop_candidate_aug25/` with a `README.md`
   stating its parameters, its scored numbers, and that it is a **candidate**, not canonical.
2. **`desktop_locked/` is not touched, renamed, or deleted at this step.** It remains the canonical
   build.
3. A second, separate approval is required to make the candidate canonical. That step would also need:
   the August `_index.md` status section rewritten, a new attribution-ledger row for the config change,
   the canonical notebook's **config-lock assertion** updated to the new parameters (it currently
   asserts s01 field-by-field and will abort otherwise), and a decision on whether to spend Dec-15 drift
   or absorb it via the headwind anchor.

Steps 1–3 are listed for completeness. **This plan covers the search only**; promotion is a separate
conversation.

---

## Documentation and tests

**Documentation to write as the search runs:**

- `research/param-scans/aug25-gap/_index.md` — what's in the dir, what isn't, where new files go.
  Must state up front that this search moves the trough **down** and must not be confused with the
  raise-the-trough scans that are its siblings under `research/param-scans/`.
- `research/param-scans/aug25-gap/LOG.md` — append-only, one entry per round, **dead ends retained**.
- `research/param-scans/_index.md` — one line added pointing at the new topic dir.
- A `README.md` in each `runs/<slug>/` recording the exact command and the scored result.

**Tests:** this search adds **no production code**, so no new unit tests are warranted — writing one
here would be decoration. The correctness checks are instead:

- `score_near_horizon.py` was verified against the canonical build and reproduces 45,223,249 /
  48,703,960 exactly. Any future edit to it must be re-verified against those two constants.
- Every candidate loads via `load_forecast(..., require_state=["l","o"])`, so a mis-gated run that
  silently dropped the overlays fails loudly instead of scoring as a plausible-looking candidate.
- The comparison notebook asserts each candidate's sidecar holds the intended parameters, and that the
  four holiday knobs and `seasonality_regime` are unchanged from s01 — enforcing the frozen-lever list
  mechanically rather than by memory.

If Round 2 or later warrants a reusable round-driver script, it goes in
`research/param-scans/aug25-gap/` (**never `tmp/`**) with a smoke test.

## Verification

Per round, before reporting:

1. `score_near_horizon.py --target-date 2026-08-25` on each new parquet; record Aug-25, Dec-15, and both
   seam-kink figures into `scores.csv`.
2. Confirm each run's sidecar `adjustments_applied` lists exactly `l` and `o`, and that the filename
   carries `.adj-lo.`.
3. Confirm each run's `parameters.json` differs from s01 only in the intended knob(s).
4. Confirm `desktop_locked/` is byte-unchanged (`git status` clean for `data-official/2026-08/`, and the
   canonical parquet's mtime and size unmoved).
5. Re-score the canonical build and assert it still reads 45,223,249 / 48,703,960 — catches any
   accidental change to the shared raw cache or the headwind spec.

---

## Open items

- **Per-run wall-clock is unknown.** Measured on `r1a`, reported, and used to size Rounds 2–4.
- Whether one knob suffices or a two-knob pairing is needed is genuinely open until Round 1's
  efficiency ratio exists.
