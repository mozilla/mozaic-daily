# Aug-25 gap-narrowing search — append-only log

Dead ends are kept. Entries are never edited after the round closes, only appended to.

---

## 2026-07-30 — Opening: target fixed

Measured the gap the task is defined against, on the desktop ALL post-headwind 28d-MA:

| date | August current | July delivered | gap |
|---|--:|--:|--:|
| 2026-08-01 | 46,753,288 | 44,910,189 | +1,843,099 |
| 2026-08-15 | 45,637,701 | 43,559,656 | +2,078,045 |
| **2026-08-25** | **45,223,249** | **43,261,424** | **+1,961,825** |
| 2026-09-15 | 47,138,508 | 45,679,737 | +1,458,771 |
| 2026-10-15 | 48,540,196 | 47,852,445 | +687,751 |
| 2026-12-15 | 48,703,960 | 48,585,483 | +118,477 |

Aug-15 was the initially stated date; the user corrected it to **Aug-25**, the trough minimum, which
is also where the current build actually bottoms out. 10% of that gap is **196,183**, so the target
is **45,027,066 ± 25,000**, with Dec-15 held inside ±50,000 of 48,703,960.

Required trade: **196,183 / 50,000 ≈ 3.92:1** Aug-25 per Dec-15. That ratio is the number the whole
search turns on.

**Tooling decision — reuse, don't build.** `scripts/score_near_horizon.py` already computes Aug-25,
Dec-15 and both seam-kink figures off a parquet, and already imports the Fix-A `display_ma`. Verified
it reproduces the canonical build at exactly 45,223,249 / 48,703,960, so it is trusted as the scorer.
`score_gradient.py` here wraps it rather than reimplementing anything; smoke-tested against s01 alone
and it read `aug25_vs_target = +196,183`, `dec15_vs_base = −0`, as it must.

**Contamination guard.** `aug22-retune/` and `summer-trough-v2/` were *not* read. Both were tuned to
move the trough **up**; importing their gradients or sampled points would import their direction.
Noted for the record: the s01 retune's widely-quoted headline (trough +1,359,887 for +5,642 at
Dec-15) is a **multi-knob** move — cps, cpr, ncp, recent_weeks and regime changed together — so it is
useless as a single-knob step-size calibration and is being used here only as evidence that the space
has leverage to spare.

---

## 2026-07-30 — Round 1: ±δ gradient probe about s01 (10 runs)

**Design.** A single-knob (cps-only) probe was drafted first and rejected on the user's read that one
knob likely won't carry a 196,183 move inside the Dec-15 budget. Replaced with a symmetric probe on
every primary knob, which buys the *ranking* of knobs by efficiency rather than one knob's slope.

Central differences about s01, one knob moved per run:

| knob | −δ | s01 | +δ | δ | ±% |
|---|--:|--:|--:|--:|--:|
| `changepoint_prior_scale` | 0.1649 | 0.1849 | 0.2049 | 0.02 | 10.8% |
| `changepoint_range` | 0.684 | 0.734 | 0.784 | 0.05 | 6.8% |
| `n_changepoints` | 30 | 35 | 40 | 5 | 14.3% |
| `prophet_recent_weeks` | 14 | 17 | 20 | 3 | 17.6% |
| `seasonality_prior_scale` | 0.0065 | 0.00825 | 0.01 | 0.00175 | 21.2% |

s01 itself is not re-run — `data-official/2026-08/desktop_locked/` is the center of every difference.
Sampling both sides gives curvature as well as slope, which matters because a one-sided slope
extrapolated to a 196,183 move would be unreliable and this parameter family has shown strong
curvature before.

Frozen on every run: `seasonality_regime=multiplicative`, all four holiday knobs at package defaults,
LOL ceiling, headwind. Raw BQ pull symlinked from the shared cache, so no query is issued and all
candidates train on byte-identical input.

Driver: `run_gradient_round1.sh`, 3 concurrent on 14 cores / 48 GB.

**Runtime.** ~1m55s per desktop run; 10 runs at 3-concurrent finished in **7.5 minutes** wall clock
(14:47:32 → 14:55:03). Cheap enough that later rounds need not be rationed.

**Driver bug, fixed.** The first invocation reported `Failures: 77357` while all 10 runs succeeded.
The throttle loop waited on `jobs -rp`, which can still report a finished-but-unreaped job after the
last `wait -n` has drained the pool; the loop then re-entered and `wait -n` returned 127 instantly,
spinning and incrementing the counter. Replaced with explicit PID tracking. Recorded because the
symptom (a huge failure count on a fully successful sweep) is exactly the kind of thing that gets
mistaken for a modelling problem.

### Results

| label | Aug-25 | Δ vs s01 | Dec-15 | Δ vs base | kink | trough date |
|---|--:|--:|--:|--:|--:|---|
| `cpr_hi` | 45,195,814 | **−27,436** | 48,694,694 | −9,266 | −10,539 | 08-25 |
| `cpr_lo` | 45,197,364 | −25,885 | 48,996,181 | **+292,221** | −10,891 | 08-25 |
| `sps_hi` | 45,199,412 | **−23,837** | 48,700,009 | −3,951 | −10,898 | 08-25 |
| `cps_lo` | 45,205,027 | **−18,222** | 48,701,333 | −2,627 | −10,616 | 08-25 |
| `sps_lo` | 45,205,492 | −17,757 | 48,689,599 | −14,361 | −10,577 | 08-25 |
| `cps_hi` | 45,208,430 | −14,819 | 48,700,443 | −3,517 | −10,236 | 08-25 |
| **`s01`** | **45,223,249** | — | **48,703,960** | −0 | −9,554 | 08-25 |
| `ncp_hi` | 45,227,461 | +4,212 | 48,698,098 | −5,862 | −8,125 | 08-25 |
| `recent_lo` | 45,229,028 | +5,779 | 48,697,263 | −6,697 | −9,312 | 08-25 |
| `ncp_lo` | 45,240,330 | +17,081 | 48,695,571 | −8,389 | −9,452 | 08-25 |
| `recent_hi` | 45,250,833 | +27,584 | 48,707,344 | +3,384 | −8,633 | 08-25 |

Figure: `plots/round1_gradient.png`.

### Four findings

**1. s01 sits at a local extremum in every knob at Aug-25, so the gradient is not usable.**
For `cps`, `cpr` and `sps`, *both* the +δ and −δ sides move Aug-25 **down** — s01 is a local maximum.
For `ncp` and `recent`, both sides move it **up** — a local minimum. Central differences therefore
mostly cancel, and the one-sided slopes disagree by ratios of **19.4 (cps), 68.8 (cpr), 13.7 (sps)**
against a >0.3 "linear solve is unsafe" threshold. Any extrapolation from these slopes is invalid.
This reads as a rough response surface — Prophet re-places changepoints discretely between fits, so
the trough responds in jumps rather than smoothly.

**2. The required move is ~7× the largest single-knob effect.** Best downward mover is `cpr_hi` at
−27,436, i.e. **14%** of the needed −196,183. Nothing sampled comes close.

**3. Dec-15 is not the binding constraint — Aug-25 responsiveness is.** Nine of ten candidates move
Dec-15 by under 15,000 against a ±50,000 budget. The whole search was framed around a 3.92:1 trade,
and the actual difficulty turns out to be elsewhere: there is Dec-15 headroom to spare and no way
found yet to spend it on Aug-25. The sole exception is `cpr_lo`, which blows the budget at
**+292,221** — the one clearly dangerous direction found, and notable because its *Aug-25* effect
(−25,885) looks almost identical to `cpr_hi`'s while its Dec-15 behaviour is completely different.

**4. Shape held everywhere.** The trough stayed on 2026-08-25 in all 10; seam kink ranged −8,125 to
−10,898 against s01's −9,554. Mild pattern worth noting: the knobs that lower Aug-25 slightly worsen
the kink, and those that raise it improve it.

### Naive stack (untested)

The three downward movers with Dec-15 headroom sum to **−69,495 at Aug-25 for −15,844 at Dec-15** —
about **35%** of the target. Cross-terms are unmeasured by a one-at-a-time design and cannot be
assumed additive; this is a hypothesis for Round 2, not a result.

### Open question for Round 2

Whether ±δ was simply too small a step, or whether Aug-25 is genuinely insensitive to these five
knobs inside the multiplicative regime. Round 1 cannot distinguish these. **Not yet concluded.**

---

## 2026-07-30 — Prior art read (user-authorised after Round 1 closed)

The contamination guard on `aug22-retune/` was lifted *after* the Round-1 entry above was written, so
that finding stands as independent. Read: its `_index.md`, `round2/FINDINGS.md`, `round3/FINDINGS.md`.

**Its objective was the opposite of ours** (lift the trough to a 45.06M bullseye) and its numbers are
not transferable — different anchor (2026-07-06 vs 07-28), different center (July's locked params vs
s01), different scored date (Aug-22 vs Aug-25). What transfers is the **map of which knobs are live**,
and our Round 1 independently reproduced its ranking.

**It answers the open question above: the step size was not the problem.** Under
`regime=multiplicative` — where s01 lives and where this search is frozen — the knobs are simply dead:

| knob | its range swept, under multiplicative | Aug effect |
|---|---|--:|
| `sps` | 0.00825 → 10 (1200×) | **inert**, ~±10K |
| `cps` | 0.05 → 0.30 (6×) | **inert**, ±≤6K |
| `recent` | 8 → 20 | +5K / −24K |
| `ncp` | 20 → 35 | +8K / +37K |
| **`cpr`** | 0.55 → 0.90 | **the only live one, ~±114K** |

So widening our ±δ steps buys almost nothing. The estimated downward ceiling under multiplicative is
**~−114K from `cpr`↑, maybe −150K stacked with `recent`↑** — against the −196,183 required. **The
target is probably not reachable with `regime=multiplicative` frozen.**

**It also explains our "s01 is a local maximum" finding.** s01 *was that search's winner* — the best of
a 28-config Latin hypercube selected to **maximise** the Aug trough. We are now trying to reduce the
very quantity s01 was chosen to maximise, so of course every direction from it goes down, and of course
the surface is flat right at the peak.

**Corroboration on `cpr`'s erratic Dec behaviour.** Its round 3 records Dec-15 Δ of +205K / −112K /
+186K at cpr 0.70 / 0.80 / 0.90 — non-monotonic by ~300K. Our Round 1 saw the same signature
(`cpr_lo` +292,221 vs `cpr_hi` −9,266 for near-identical Aug effects). `cpr` must be sampled densely,
never interpolated.

### The lever that would work, and why it is out of scope

Its round 2 measured `seasonality_regime` as a **shape** lever: multiplicative vs auto/additive is
**+1.29M Aug for +0.19M Dec — a 6.7:1 asymmetry**, categorically better than sps's symmetric 1:1.4.
Running that axis *backwards* is exactly what this search wants, and we need only **15%** of its swing.
At 6.7:1 the implied Dec cost of −196,183 is about **−29,000** — comfortably inside ±50,000.

`--seasonality-corr-threshold` makes that axis **continuous** (per-tile cutoff on corr(|y|,|dy|);
lower ⇒ more tiles multiplicative), which is precisely the fine dial this search lacks. It requires
`regime=auto`.

Both are **frozen levers** under this search's contract, so nothing has been run. Note two things for
whoever decides: (a) its round 2 flagged that forced-global-multiplicative overrides the data-driven
per-tile regime for ~40% of tiles, a generalizability concern — so moving toward `auto` would move
*toward* the more defensible config, not away; (b) the empirical calibration of `corr_threshold` is
unknown (auto@0.0 measured ≈ additive) and would have to be measured, not assumed.

---

## 2026-07-30 — July's shipped config, measured at OUR anchor

`data-official/2026-08/desktop_baseline_2026-07-28/` is July's parameter set run at the August
anchor on August data, so it isolates config from data refresh. July's `parameters.json` has **no
`seasonality_regime` field at all** — the knob post-dates that build — so it ran the package default,
confirmed to be **`auto`**.

| | Aug-25 | Dec-15 | kink |
|---|--:|--:|--:|
| s01 (canonical) | 45,223,249 | 48,703,960 | −9,554 |
| July params @ 07-28 | 43,833,674 | 48,672,970 | −65,344 |
| **delta** | **−1,389,575** | **−30,990** | −55,790 |

July's config already satisfies the Dec-15 constraint while overshooting Aug-25 by 7×: a **44.8:1**
lever. The target is 14.1% of the way along that axis. (That build carries LOL 180K vs the locked
200K; the ledger's pinned steps put the corrected Dec-15 delta near −23,779, strengthening the ratio
to ~58:1. Not silently applied — the correction is a pinned constant, not a measurement of this build.)

Config differences: `regime` auto→multiplicative, `cps` 0.08983→0.1849, `cpr` 0.65→0.734, `ncp`
25→35, `recent` 13→17. `sps` and all four holiday knobs identical.

**Regime added to the search** on the user's instruction, conditional on it being the difference.

---

## 2026-07-30 — Round 2: blend axis s01 → July params, both regimes (8 runs)

**Deviation from the agreed design, flagged before running.** The approved option said "regime
switching at the midpoint", but every sampled fraction is ≤ 0.30, so a midpoint rule would have
pinned all 8 runs to multiplicative — where Round 1 and the prior art both cap the reachable move
near −150K. Ran **both regimes at each fraction** instead (8 runs, 6 min), which also measures the
regime step at our own center rather than inheriting −1.29M from a different center and anchor.

`sps` is not interpolated: both ends are 0.00825 (the CLI default is 0.00825, which is what July's
unset field resolved to), so there is no axis to travel.

| f | regime | Aug-25 | vs s01 | Dec-15 | vs base | kink |
|--:|---|--:|--:|--:|--:|--:|
| 0.00 | multiplicative | 45,223,249 | 0 | 48,703,960 | −0 | −9,554 |
| 0.10 | multiplicative | 45,247,957 | +24,708 | 48,694,719 | −9,241 | −7,477 |
| 0.15 | multiplicative | 45,202,918 | −20,331 | 48,724,851 | +20,891 | −9,424 |
| 0.20 | multiplicative | 45,218,046 | −5,203 | 48,757,377 | **+53,417** | −8,240 |
| 0.30 | multiplicative | 45,195,892 | −27,357 | 48,842,781 | **+138,821** | −9,458 |
| 0.10 | auto | 43,869,912 | −1,353,337 | 48,696,827 | −7,133 | −64,287 |
| 0.15 | auto | 43,867,133 | −1,356,116 | 48,696,629 | −7,331 | −64,310 |
| 0.20 | auto | 43,878,302 | −1,344,947 | 48,707,455 | +3,495 | −63,805 |
| 0.30 | auto | 43,860,285 | −1,362,964 | 48,691,753 | −12,207 | −64,504 |
| 1.00 | auto | 43,833,674 | −1,389,575 | 48,672,970 | −30,990 | −65,344 |

Figure: `plots/round2_blend_gap.png`. **Candidates satisfying both constraints: 0.**

### The regime is a cliff, and the target is in the gap

Regime step (auto − multiplicative) at matched trend knobs: **−1,378,045 / −1,335,785 / −1,339,744 /
−1,335,607** at f = 0.10 / 0.15 / 0.20 / 0.30. Essentially constant at ~−1.35M — the step does not
depend on the trend knobs, so no blend fraction modulates it.

- multiplicative floor: **45,195,892**
- auto ceiling: **43,878,302**
- gap: **1,317,589** wide
- **target 45,027,066 sits inside it** — 168,826 below the multiplicative floor, 1,148,764 above the
  auto ceiling.

**Blended interpolation cannot reach the target.** This is a structural result, not a sampling
artifact: the axis is discontinuous and the target lands in the discontinuity.

Two supporting observations:

1. **The multiplicative branch runs out of Dec-15 budget before it runs out of Aug-25 travel.** It
   exits ±50,000 at f=0.20 (+53,417) and is far out at f=0.30 (+138,821), having moved Aug-25 by only
   −27,357. So pushing further along multiplicative is doubly blocked.
2. **The auto branch has Dec-15 headroom everywhere** (−30,990 to +3,495) and is limited purely by
   overshooting Aug-25. All the difficulty is on one axis.

**Kink tracks the regime, not the fraction:** ~−7,477 to −9,458 across the whole multiplicative
branch, ~−63,805 to −65,344 across the whole auto branch. So the kink guardrail is really a *regime*
guardrail.

### Round 3 implication

The only continuous crossing of the gap is **`seasonality_corr_threshold` under `regime=auto`** — it
sets the per-tile cutoff on corr(|y|,|dy|) deciding which tiles run multiplicative, so it dials the
*mixture* rather than flipping a global switch. Landing the target requires sitting **~87.2%** of the
way from the auto ceiling toward the multiplicative floor. Its calibration is unknown and must be
measured; `corr_threshold = −1.0` should reproduce forced-multiplicative and is the natural sanity
anchor for the sweep.

---

## 2026-07-30 — Integrity check: `lol.json` changed mid-search (benign)

Sidecar audit found two `l` spec hashes across the runs: `e8b4a218` on Round 1's 10, `a6c2ed01` on
Round 2's 8. `o` was identical across all 18. A spec change mid-search would normally invalidate
cross-round comparison.

`git diff` shows the change is **notes-only** — `data_file` is `lol_tailwind.2026-07-29.cap200k.parquet`
on both sides; the rewrite records that the 125K/165K/180K alternates were deleted 2026-07-30. **All
runs share the same 200K curve; Rounds 1–3 are mutually comparable.**

One consequence: the f=1.0 endpoint (`desktop_baseline_2026-07-28/`) has LOL **180K** baked in, and
that curve file is now deleted. The build is frozen and its numbers stand, but it is no longer
reproducible from the working tree — only from git history. Nothing downstream depends on re-running it.

---

## 2026-07-30 — Round 3: `seasonality_corr_threshold` calibration (6 runs)

regime=auto, s01 trend knobs held fixed, t ∈ {−1.0, −0.8, −0.6, −0.4, −0.2, 0.0}. 6/6 succeeded, ~2m
each.

| t | Aug-25 | Dec-15 | kink |
|--:|--:|--:|--:|
| −1.0 | 45,223,249 | 48,703,960 | −9,554 |
| −0.8 | 45,223,249 | 48,703,960 | −9,554 |
| −0.6 | 45,223,249 | 48,703,960 | −9,554 |
| −0.4 | 45,223,258 | 48,703,787 | −9,553 |
| −0.2 | 45,222,105 | 48,708,527 | −9,417 |
| **0.0** | **43,866,228** | 48,685,692 | **−64,437** |

Figure: `plots/round3_corr_dial.png`.

**Sanity anchor passed exactly.** t=−1.0 reproduces forced-multiplicative at drift **+0** — the dial
means what the help text says, so the rest is interpretable.

### The dial is not a ramp either — it is the same cliff, relocated

t from −1.0 to −0.4 is byte-identical to s01. t=−0.2 moves Aug-25 by only −1,144. **The entire
1,355,877 transition happens between t=−0.2 and t=0.0**, which this sweep did not sample.

What that implies about the data: at t=−0.2 essentially every tile is above the cutoff (all
multiplicative), and at t=0.0 essentially every tile is below it. So **virtually every tile's
corr(|y|,|dy|) lies in (−0.2, 0.0]** — the whole discriminating range is a 0.2-wide window and the
first sweep stepped straight over it.

**The interpolated crossing at t ≈ −0.171 is NOT a solution** and is not reported as one. The
bracketing samples are 1,355,877 apart — **54× the ±25,000 tolerance** — so the interpolation is a
guess about the interior of an unsampled step. `solve()` was amended to return the bracket height and
the scorer now prints an explicit `*** NOT A SOLUTION ***` warning and greys the step on the figure,
because a plausible-looking interpolated threshold is exactly the kind of number that gets quoted.

### Kink

Flat at ~−9,554 from t=−1.0 to −0.4, −9,417 at −0.2, then −64,437 at 0.0. Same structure: the kink
does not vary along the dial, it jumps with the mixture. The interpolated −17,331 at the nominal
crossing is subject to the same caveat and is unverified.

### Dec-15 is comfortable throughout

Range −0.25 to +4,567 across t ∈ [−1.0, −0.2], and −18,268 at t=0.0 — all well inside ±50,000. Dec-15
has not been the binding constraint at any point in this search.

### Round 4 implication

Sample densely **inside (−0.2, 0.0)**. Only 14.5% of the available 1,355,877 drop is needed, so the
threshold should sit near the −0.2 end where few tiles have flipped. **Open risk:** the staircase is
per-tile, so if one large tile (e.g. US) carries a step bigger than 50,000 there may be *no* threshold
that lands inside ±25,000. A dense sweep will show the step sizes and settle it.

---

## 2026-07-30 — Round 4: dense sweep inside (−0.2, 0.0) + 3 bisections (11 runs)

8-point sweep at t ∈ {−0.18, −0.16, −0.14, −0.12, −0.10, −0.08, −0.05, −0.02}, then the driver
bisected the straddling step three times autonomously (authorised in-round). 11/11 succeeded, ~2.8m
each.

| t | Aug-25 | Dec-15 | kink |
|--:|--:|--:|--:|
| −1.0000 … −0.4000 | 45,223,249 | 48,703,960 | −9,554 |
| −0.2000 | 45,222,105 | 48,708,527 | −9,417 |
| −0.1800 | 45,222,105 | 48,708,527 | −9,417 |
| −0.1600 | 45,228,935 | 48,710,162 | −9,259 |
| −0.1500 | 45,228,935 | 48,710,162 | −9,259 |
| **−0.1450** | **45,227,524** | 48,710,162 | −9,233 |
| **−0.1425** | **43,869,632** | 48,685,661 | **−64,301** |
| −0.1400 … 0.0000 | 43,866,228 – 43,870,074 | ~48,685,670 | ~−64,400 |

Figure: `plots/corr_dial_full.png`. Scores: `corr_dial_scores.csv`. **Candidates: 0.**

### It is one riser, not a staircase

The bisection did not subdivide the step. It stayed **1,357,891 DAU tall** while the bracket narrowed
from 0.02 → 0.01 → 0.005 → **0.0025** wide. The entire transition happens between **t = −0.1450 and
t = −0.1425**.

So the per-tile mixture hypothesis is **wrong in effect**: essentially every tile flips regime at the
same threshold, around t ≈ −0.1435. There is no intermediate mixture to sit in. `corr_threshold` is
not a dial — it is a relabelled switch.

Two secondary observations, neither of which changes the verdict:

- Movement *within* each plateau is 1,000–7,000 DAU (e.g. −0.20 → −0.16 is +6,830), i.e. a handful of
  tiny tiles do flip individually. They are ~3% of what the target needs.
- The plateaus are not perfectly flat and not monotone (−0.16 reads *above* −0.20). That is refit
  noise of the same order as the tiny-tile effects, and it means even the small movements are not
  reliably steerable.

**Reporting-precision note.** The first table rendered `−0.1450`, `−0.1425` and `−0.1400` all as
"−0.14", which made the two sides of the cliff look like one repeated row. Fixed to 4 decimals. The
plot and CSV were renamed `corr_dial_full` since they now span Rounds 3–4.

### Verdict on the regime axis

**The seasonality axis cannot deliver −196,183.** Its only operating points at Aug-25 are ~45,223,000
(multiplicative side) and ~43,868,000 (auto side). The target 45,027,066 lies between them with
nothing in between, and four rounds have now failed to find an interior point because there is none.

### What is NOT yet ruled out

Aggressive `changepoint_range` under multiplicative, stacked with `sps` and `cps`. Round 1 measured
`cpr_hi`(0.784) at −27,436 with only ±δ; the prior art swept `cpr` to 0.90 and saw ~−114K at its own
center. Naively stacking the three safe downward movers gave −69,495; a more aggressive cpr could
plausibly push that toward −150K. **That is still short of the −171,183 needed to reach the bottom of
the accept band**, and cross-terms are unmeasured — but it is the one untested direction, and it is
cheap. Stated here so the negative result is not overclaimed.

---

## 2026-07-30 — Round 5: aggressive cpr under multiplicative + stacks (6 runs)

Phase 1, cpr ladder (s01 elsewhere, multiplicative). Phase 2, the best in-budget ladder point
stacked with the other two downward movers. 6/6 succeeded, ~1.8m each.

| config | Aug-25 | vs s01 | Dec-15 Δ | kink | in budget |
|---|--:|--:|--:|--:|:-:|
| `cpr0.784` (Round 1) | 45,195,814 | **−27,436** | −9,266 | −10,539 | ✓ |
| `cpr0.82` | 45,216,795 | −6,454 | +15,804 | −10,483 | ✓ |
| `cpr0.86` | 45,330,907 | **+107,658** | +284,733 | −7,322 | ✗ |
| `cpr0.90` | 45,330,289 | +107,040 | +914,452 | −8,335 | ✗ |
| `cpr0.94` | 45,607,595 | +384,346 | +2,729,781 | +1,504 | ✗ |
| `cps0.1649_cpr0.82` | 45,265,367 | +42,118 | +68,301 | −8,521 | ✗ |
| `cps0.1649_cpr0.82_sps0.01` | 45,204,500 | −18,749 | +11,703 | −10,870 | ✓ |

### Three findings, all negative

**1. cpr reverses past 0.784.** It is not monotone: −27,436 at 0.784, −6,454 at 0.82, then **+107,658
at 0.86** and +384,346 at 0.94 — moving Aug-25 the *wrong* way. Dec-15 simultaneously explodes
(+284,733 → +914,452 → +2,729,781). There is no aggressive-cpr regime to exploit; 0.784 was already
past the useful maximum.

**2. The prior art's cpr result did not transfer, as flagged.** `aug22-retune/round3` measured cpr
0.90 at ~−114K Aug with Dec ~+186K at *its* center and anchor. At ours it is **+107,040 Aug and
+914,452 Dec** — opposite sign on Aug and 5× the Dec damage. The transferability caveat recorded when
that scan was read was the right call; only its *ranking* of which knobs are live survived the move.

**3. Stacking is strongly non-additive and partially cancelling.** Naive sum of the three best
single-knob movers: **−69,495**. Measured 3-knob stack: **−18,749** — worse than `cpr0.784` alone.
The 2-knob `cps0.1649 + cpr0.82` is worse still at **+42,118**, i.e. combining two individually
*downward* knobs moved Aug-25 *up* and blew the Dec-15 cap. The knobs do not compose.

This overturns the estimate offered before the round that stacking might reach 35–50% of the target.
It reaches 14%, and the best config remains a single knob.

### Parametric ceiling — final

**Best achievable: `cpr = 0.784`, everything else at s01.**

| | value |
|---|--:|
| Aug-25 | **45,195,814** |
| vs s01 | **−27,436** |
| **share of the required −196,183** | **14.0%** |
| vs target | +168,748 (outside the ±25,000 band) |
| Dec-15 | −9,266 (well inside ±50,000) |
| seam kink | −10,539 (−986 vs s01 — negligible) |
| trough date | 2026-08-25, unmoved |

42 configs scored across five rounds. **0 hit the target. 35 held Dec-15 in budget** — Dec-15 was
never the binding constraint at any point.

---

## 2026-07-30 — Round 6: full 3^5 factorial around the Round-5 best (243 cells)

Centre = the Round-5 winner (cps 0.1849, cpr 0.784, ncp 35, recent 17, sps 0.00825, multiplicative).
Levels: cps ±0.02, **cpr ±0.03** (tightened from Round 1's ±0.05 because the centre moved to 0.784
and cpr 0.86 was measured at Dec-15 +284,733), ncp ±5, recent ±3, sps ±0.00175. regime held
multiplicative. **243/243 cells complete.**

**Operational note — 58 cells lost to expired credentials.** The first run took 191m and lost 58
cells to ADC expiry (57 `RefreshError`, 1 `bigquerystorage` 401); zero modelling failures. The driver
treated each as an ordinary per-cell failure, so it kept spawning subprocesses that failed fast and
the run looked healthy while a quarter of it evaporated. Fixed: `check_credentials()` refreshes ADC
before the first cell, and an auth error now aborts the whole run rather than being retried per cell.
Relaunched after re-auth; skip-existing meant only the 58 re-ran (28m). **Every cell needs live
credentials** because `run_param_scan` issues a BigQuery pre-flight check even on a cached raw pull.

### THE TARGET IS REACHABLE — 1 cell of 243 hits it

**`cps 0.1649, cpr 0.814, ncp 40, recent 17, sps 0.00825`** (multiplicative, holiday knobs default)

| | value | status |
|---|--:|---|
| **Aug-25** | **45,036,389** | **+9,323 vs target — INSIDE ±25,000** |
| Dec-15 | 48,672,603 | −31,357 — inside ±50,000 (63% of budget) |
| seam kink (model-only) | −16,549 | −6,996 vs s01; **1.73×** worse |
| trough date | 2026-08-25 | unmoved |
| move from canonical s01 | **−186,860** | **95.2%** of the required 196,183 |

Verified independently of the analysis cache by re-scoring the parquet directly.

### But it is a solitary spike, not a basin

**All ten one-step neighbours sit ABOVE it, by 52,092 to 165,860.** Mean neighbour is +80,271.

| changed knob | → | Aug-25 vs winner |
|---|---|--:|
| cps 0.1649 → 0.1849 / 0.2049 | | +52,092 / +52,555 |
| cpr 0.814 → 0.784 / 0.754 | | +165,860 / +86,931 |
| ncp 40 → 35 / 30 | | +138,841 / +66,982 |
| recent 17 → 14 / 20 | | +66,981 / +55,146 |
| sps 0.00825 → 0.0065 / 0.01 | | +57,824 / +59,501 |

Only **1 of 243** cells lands in the accept band; 35 fall below 45,100,000. Grid Aug-25 spread is
186,094 (45,036,389 … 45,222,483).

Two readings, and the grid cannot separate them: either the cpr:ncp interaction has a genuine narrow
optimum here, or this is the extreme order statistic of 243 draws from a rough surface. Prophet is
deterministic for a fixed config (Round 3 produced byte-identical results for effectively-identical
configs), so the cell **will** reproduce — the question is not reproducibility but **robustness**: a
one-grid-step move in *any* of the five knobs costs at least +52,092. **Not recommended for adoption
without a fine local sweep.**

### Variance decomposition — exact, all 31 effects, asserted to sum to 100%

**Aug-25**

| effect | order | % variance |
|---|--:|--:|
| `ncp` | 1 | **40.56** |
| `cpr` | 1 | **31.27** |
| `cpr:ncp` | 2 | **18.48** |
| `recent` | 1 | 1.15 |
| `cps:cpr:ncp:recent:sps` | 5 | 1.05 |

Main effects **73.0%** / interactions **27.0%**. Those top three account for **90.3%**.

**Dec-15**

| effect | order | % variance |
|---|--:|--:|
| `ncp` | 1 | 23.05 |
| `cpr:ncp` | 2 | 21.60 |
| `cpr` | 1 | 19.04 |
| `cps:cpr:ncp:recent:sps` | 5 | 4.52 |

Main effects **45.1%** / interactions **54.9%** — Dec-15 is *majority* interaction-driven.

### Four findings the one-at-a-time rounds could not have produced

**1. `ncp` is the dominant Aug-25 factor (40.6%) and Round 1 called it inert.** Round 1's ±δ probe at
s01 (cpr 0.734) measured ncp at +4,212 / +17,081 — both *upward*, seemingly useless. At cpr 0.814 it
is the strongest lever in the design. Its potency is conditional on cpr, which a one-at-a-time probe
is structurally blind to.

**2. `cpr:ncp` is a first-class effect (18.5% of Aug-25, 21.6% of Dec-15).** This is the direct
measurement of what Round 5 failed to guess: stacking individually-downward knobs gave −18,749 when
the naive sum said −69,495, and one pair moved Aug-25 *up*. The interaction was never small; it was
never measured.

**3. Correlation badly understates importance, in a specific way.** `ncp` has a linear correlation
with Aug-25 of only **+0.110** while carrying **40.6%** of its variance — its effect is quadratic and
conditional, so a linear statistic nearly misses it. Anyone screening these knobs by correlation
would have discarded the most important one.

**4. `cps` and `sps` are inert as main effects** (correlations +0.002 and +0.007 with Aug-25; neither
appears in the top main effects) yet both appear in the large high-order terms. They do nothing on
their own and only matter in combination.

**Caveat on the high-order terms.** With one observation per cell and no replication, the 3/4/5-way
terms (≈5% of Aug-25, ≈29% of Dec-15) absorb surface roughness as well as genuine structure. They are
deterministic and real, but should not be read as smooth exploitable mechanism. Centre replicates
were offered in the design and not taken, so the noise floor is not directly measured; the Round-4
plateau wobble (~7K) is the best available proxy and is smaller than these terms but of the same
order.

---

## 2026-07-30 — Objective changed: triple constraint

Set by the user after Round 6, replacing the single Aug-25 objective:

| | rule |
|---|---|
| Aug-25 | within **±75,000** of 45,027,066 — **hard** (widened from ±25,000) |
| Dec-15 | within ±50,000 of 48,703,960 — **hard**; cells above 80% of cap (>40,000) flagged, not preferred |
| **seam kink** | **minimise the increase vs s01's −9,554** — now the objective |

Implemented in `select_candidate.py`. `leaderboard.py` is kept unchanged as the record of how rounds
1–6 were judged; editing it would have made the earlier entries here unreproducible.

Applying the new rule to Round 6's existing grid immediately beat the Round-6 winner: 35 feasible
cells, best kink penalty **+4,176** against the winner's +6,996. But every Pareto point sat at
`cpr=0.814` and `ncp=40` — both upper edges — with the best-kink ones also at `recent=20`, a third
edge. Three of five knobs pinned at a boundary motivated Round 7.

---

## 2026-07-30 — Round 7: extend past the pinned edges (108 cells)

`cps {0.1749, 0.1849, 0.1949} × cpr {0.814, 0.829, 0.844} × ncp {40, 43, 46, 49} × recent {20, 23, 26}`,
`sps` held at 0.00825. 108/108 succeeded in 88m (~127s/cell, slower than Round 6's ~112s because the
region uses higher `ncp`). `cpr` stopped at 0.844 because Round 5 measured 0.86 at Dec-15 +284,733.

**`sps` was held, not varied** — a deviation from a full extension, to fit 108 cells. Justified by
Round 6's decomposition: `sps` correlates +0.007 with Aug-25, is absent from the top effects, and the
two adjacent Pareto points differing only in `sps` are 224 apart in kink penalty.

### The extension hypothesis was wrong, and that is the result

| extension level | cells | feasible | Dec-15 range |
|---|--:|--:|---|
| `cpr` 0.829 | 36 | **0** | +41,889 … +284,054 |
| `cpr` 0.844 | 36 | **0** | −105,248 … +191,362 |
| `ncp` 43 | 27 | **0** | −105,248 … +251,886 |
| `ncp` 46 | 27 | **0** | −21,932 … +284,054 |
| `ncp` 49 | 27 | **0** | −36,817 … +236,506 |
| `recent` 23 / 26 | 72 | 6 | — |

Of 111 extension cells, **6 are feasible, all from `recent`, and none improves on the best kink**.
Raising `cpr` or `ncp` past the Round-6 edge blows Dec-15 in **every single case**. The pinning was
the constraint boundary biting, not a truncated optimum. **That question is closed** — with the
caveat that the sweep only extended *upward*, since Round 6 sampled below at coarse resolution and
found nothing Pareto-optimal there.

### The structural finding: the kink has a floor

Across **all 43 feasible configs out of 370 scored**, kink penalty spans **+4,176 … +6,996**. No
feasible config returns the kink near s01's −9,554. **A ~1.44× seam-kink regression is the price of
admission** for any config that moves Aug-25 meaningfully. This is a property of the feasible region,
not of the search.

### Selected candidate

Strictly minimising kink picks `cps 0.2049, cpr 0.814, ncp 40, recent 14, sps 0.01` at kink penalty
+4,274 — but that sits at **99% of the Aug-25 hard band**, reproducing on the Aug-25 axis exactly the
near-cap problem the margin preference was added to avoid on Dec-15. Flagged rather than adopted.

**Chosen (approved): `cps 0.2049, cpr 0.814, ncp 40, recent 14, sps 0.00825`**

| metric | value | vs canonical | budget used |
|---|--:|--:|--:|
| Aug-25 | 45,094,241 | −129,008 | 90% of ±75,000 |
| Dec-15 | 48,704,340 | **+380** | **1% of ±50,000** |
| kink | −14,285 | −4,731 | 1.50× |
| move achieved | — | — | **65.8%** |

457 of extra kink penalty buys margin on both hard constraints and takes Dec-15 to essentially zero
drift, leaving the headwind lever entirely in reserve.

Staged at `data-official/2026-08/desktop_candidate_aug25/`, replacing the Round-5 `cpr0.784`
candidate. **`desktop_locked/` untouched throughout** (mtime still 2026-07-29 19:50).

**Pickle note:** Round 6/7 pkls were deleted per-cell for disk, so the winning config was re-run to
regenerate its fitted model. The re-run reproduced the parquet's scores **exactly** (45,094,241 /
48,704,340), which incidentally confirms Prophet is deterministic for a fixed config here — so the
Round-6 winner's isolation was a robustness property, never a reproducibility one.

---

## 2026-07-30 — ADOPTED: g01 is the canonical August desktop config

The Round-6 grid winner was promoted to canonical, replacing s01.

**g01** = `cps 0.1649, cpr 0.814, ncp 40, recent 17, sps 0.00825`, multiplicative, holiday knobs at
defaults. Named for the 3^5 **g**rid that found it (s01 was named for the summer-trough search).

**Two changes, applied and reverted as one unit:**

| | from | to |
|---|---|---|
| desktop model config | s01 | **g01** |
| `h` Win10 desktop anchor | −1,245,000 | **−1,220,000** |

The +25,000 attenuation absorbs most of the config's −31,357 Dec-15 drop. Because the ramp is only
20% accrued at Aug-25 (28 of 140 days), it gives back just **+5,000** at the trough while buying
**+25,000** at Dec-15 — an asymmetry that works in our favour.

### Published result (notebook re-executed, all 22 cells)

| | Aug current | Jul delivered | delta |
|---|--:|--:|--:|
| Desktop Dec-15 | **48,697,603** | 48,585,483 | +112,120 (+0.23%) |
| Mobile Dec-15 | 17,924,607 | 17,923,869 | +738 |
| **ALL Dec-15** | **66,622,210** | 66,509,352 | +112,858 (+0.17%) |

Aug-25 trough **45,041,389** (Aug-22: 45,091,364). Seam kink −16,549.

**All notebook assertions pass:** config lock matches g01 (6 params) and holiday knobs are at
defaults; the prior July curve reproduces at **drift +0**; the attribution ledger closes to
**residual −0** with two new rows (g01 retune −31,357; headwind now carrying both attenuations,
+125,000). CSV export ran — `csv/august_canonical_curves.csv` and `august_dec15_summary.csv` are
now written, a deliberate change from the previously deferred state.

### Revert

`data-official/2026-08/desktop_s01_REVERT_2026-07-29/` holds the complete s01 build, the headwind
spec that went with it, the README it carried while canonical, and `REVERT.md`. **It is a revert
target, not an archive**; a revert is considered a live possibility because g01 is an isolated
optimum whose depth will not survive a parameter shift. Documented in `data-official/2026-08/_index.md`,
`desktop_locked/README.md`, and a new `### Revert targets` section in the project `CLAUDE.md`.

### Superseded

`data-official/2026-08/desktop_candidate_aug25/` holds the 6.58% staged candidate
(`cps 0.2049, ncp 40, recent 14`) that was **not** chosen. Retained for now as the record of the
alternative that was weighed; it is not a revert target and nothing depends on it.
