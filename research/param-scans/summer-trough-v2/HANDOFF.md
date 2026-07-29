# Handoff: desktop parameter search to lift the 2026 summer trough (August baseline)

Continuation of `../aug22-retune/` — same target, but against the August build and after the headwind
seam fix, both of which moved the starting point. Read `../aug22-retune/_index.md` and its three
`FINDINGS.md` before planning probes; the per-lever slopes at this exact center are already measured and
should not be rediscovered.

---

## 1. Goal

Raise the desktop summer trough while holding Dec-15 near its current value.

| quantity | center (regime=auto) |
|---|--:|
| **trough minimum, post-headwind** — the scored KPI | **43,833,674** on 2026-08-25 |
| Dec-15 28d-MA, post-headwind | **48,672,970** ← hold this |
| Aug-22 28d-MA, post-headwind (July's date, no longer scored) | 43,921,488 |

**Target: the 45M–46M band; exact value depends on what turns out to be achievable** (confirmed
2026-07-29). July's **45.06M ±0.1M** bullseye is **retired** — it was never an external benchmark, just the
most achievable value under July's data and knob ranges.

**Score the Aug-25 trough minimum, not Aug-22.** Aug-22 sits inside the 27-day post-seam splice zone where
the value is ~41K sensitive to the smoothing convention; Aug-25 is exactly 28 days past the seam, so its
window is entirely forecast and the number is convention-independent.

**Also report — but do not score — the 28d-MA derivative either side of the seam.** A matching handoff
slope is wanted where it can be had, but raising Aug-25 and holding Dec-15 are the goals it yields to.
`score_near_horizon.py` prints this. Note a hard floor: the headwind ramp contributes −8,893/day of slope
kink (`desktop_dau / (anchor − start)`) that no parameter can touch.

✅ **Phase 0 and phase 1 are done — see `phase1/FINDINGS.md`.** `regime=multiplicative` alone landed the
trough **in band at 45,140,569** (+1,306,895) for **+252,550** on Dec-15, and cut the model's seam slope
kink 68%. The open question is clawing that Dec-15 drift back.

## 2. What is already settled — do not re-litigate

**The seam discontinuity is fixed, and it was never a model problem.** It was 100.9% the display-layer
Win10 headwind, which ramped from 2026-04-01 but was only *applied* from the forecast seam forward, so it
switched on at 45.7% of full value (−569,419) as a one-day step. The raw model output is continuous across
the seam (+5,157). `headwind.json` now ramps from the seam, which lifted Aug-22 by **+467,737 with zero
change at Dec-15**.

Measurement, plus the `l`/`o` overlay isolation: `data-official/2026-08/desktop_adjustment_ladder.ipynb`.

**Parameters cannot move a display-layer step.** Don't aim at it.

### The build you are searching from

```
data-official/2026-08/desktop_baseline_2026-07-28/
  cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/
  mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet
```

Locked center: `cps=0.08983, cpr=0.65, ncp=25, recent=13, holiday_threshold=-0.032, max_r=5, min_r=3,
floor=-0.6, sps=0.00825, regime=auto`. Anchor 2026-07-28, trained through 2026-07-27.

## 3. What July did, and why expanding the knob list worked

`scripts/run_desktop_gradient.py` holds the actual probe lists. The structure was:

| round | design |
|---|---|
| 1 | one-at-a-time ±δ over **all 8** knobs → rank the levers |
| 2 | top-3 only (`holiday_threshold`, `cps`, `cpr`), larger δ, **plus combos** of the KPI-raising directions — measured against the naive sum to expose interaction |
| 3 | push `cps`/`cpr` into their low ranges; finely map the jumpy threshold |
| 4 | `cps` × `cpr` 2-D grid with threshold **locked** at center |

Result: `cps=0.08983`, `cpr=0.65`.

**The win came from a newly exposed knob.** `changepoint_range` had just been added to `ModelConfig`
(mozaic commit `d0f8a78`, which also added `n_changepoints`), and it turned out to be a genuine lever
where `cps` and `recent_weeks` were nearly inert. `../aug22-retune/` then expanded again with
`seasonality_prior_scale` (`126fe14`) and `seasonality_regime` (`6f02912`).

The transferable lesson is the *discipline*, not the knob list: rank cheaply over everything first, then
spend compute only on the live levers. Round 1 tested 8 knobs and discarded 5.

## 4. Slopes already measured at this exact center

From `../aug22-retune/round{1,2,3}/FINDINGS.md` — anchor 2026-07-06, same locked center, trough at Aug-22
post-headwind. Center then: trough 43,246,576 / Dec-15 48,585,483.

| lever | trough effect | Dec-15 effect | verdict |
|---|---|---|---|
| `cps`, `cpr`, `recent_weeks`, `ncp` (regime=auto) | each **< ±80K**, mostly non-monotonic | small | cannot close a 1M+ gap |
| `seasonality_prior_scale` | **huge**: 0.00825 → 0.003 gave **+2.04M** (43.25 → 45.29M) | **catastrophic**: 48.59M → **45.67M** | ~1:1.4 trade; unusable alone |
| `seasonality_regime=multiplicative` | **+1.29M** | **+0.19M** | best asymmetry found (~6.7:1), but plateaued ~44.54M and Dec sat +160–240K high |
| `cpr` **under multiplicative** | ~±100–115K (`cpr` down raises trough) | +241K at cpr 0.55 | the only live trend knob there |

July's conclusion: **no exposed combination hit Aug 45.06M while holding Dec within 10k.** Its
recommendation was a bidirectional summer-trough overlay instead.

**Why multiplicative behaves so differently:** on desktop the regime is coupled to growth
(`additive → logistic`, `multiplicative → linear`), so switching it also turns off the 426-day logistic
cap. That cap has a known artifact history — see the `logistic-cap-artifact` note in project memory.

## 5. The hypothesis to test first

Multiplicative's **+1.29M was measured from a 43.25M center**. This build's center is 43.92M, and roughly
**0.47M of that gain is display-layer** (the headwind re-anchoring), which does not touch the model at
all. So the regime's model-level effect should still be available and stack roughly additively.

**Expected landing: ~45.2M — at or above July's bullseye — for ~+0.2M on Dec-15.**

If that holds, the "parameters can't do it" verdict flips and this is close to a one-probe job. It costs
one ~15-minute run to find out, so **do this before designing any grid.**

Verify rather than assume: a regime switch is a shape change, and additivity across baselines is not
guaranteed.

## 6. Suggested phasing

0. **Re-measure the center** with the current spec and scorer settings. Confirms the scorer agrees with
   the canonical notebook and establishes the real gap.
1. **`regime=multiplicative` alone.** Read trough *and* Dec-15.
2. **If Dec drifts high**, claw it back with `cpr`, or with a tightly bounded `sps` move — check the sign
   at this center first. Use `sps` only for a final ~100–200K; it is the Dec-killer.
3. **`cpr` fine grid** under the winning regime (~±100K resolution).
4. **If both bands still cannot be held, stop and report the negative result.** The fallback is a
   bidirectional summer-trough overlay — that needs an explicit human go/no-go and must **not** be built
   unilaterally. Target shape: `../../summer-slump/`.

## 7. If the exposed set stalls

`run_param_scan.py` already covers **all 10** `DesktopModelConfig` fields, so there is nothing further
available consumer-side. The promising *unexposed* Prophet knobs, in order of relevance:

1. **Yearly-seasonality Fourier order.** `mozaic/models.py` sets `yearly_seasonality=True`, i.e. Prophet's
   default order 10. This is the one worth asking for: it changes seasonal **shape** independently of
   amplitude, which is precisely the problem with `sps` (amplitude-only, so it trades summer against
   December symmetrically).
2. `holidays_prior_scale` — Prophet default; Dec-15 sits near holidays.
3. The **426-day logistic cap window** (hardcoded) — only active under logistic growth.

⚠️ These need commits in `~/work/mozaic-forecasting-official`, which has 9 unpushed commits and is
**off-limits** without a separate decision. Raise it; don't edit it.

## 8. Tooling — and what must be repointed before use

| script | state |
|---|---|
| `scripts/run_param_scan.py` | One config per invocation; applies `l`+`o` as in the canonical build; writes `<results-dir>/<slug>/`. Also has `--no-launch-on-login` / `--no-mozillaonline` for overlay isolation. Ready to use. |
| `scripts/run_desktop_gradient.py` | Round driver. `RESULTS_ROOT`, `FORECAST_START` (2026-07-06) and every `ROUND*` probe list are July's. **Repoint before reuse.** |
| `scripts/run_aug_trough_gradient.py` | The `aug22-retune` driver; same repointing needed. |
| `scripts/score_near_horizon.py` (+4 passing tests) | The scorer. **See the warning below.** |

✅ **`score_near_horizon.py` was repaired in phase 0** — three defects, all of which mis-scored silently.
Full detail in `phase1/FINDINGS.md`; in brief:

1. `DEFAULT_HEADWIND` pointed at July's spec → wrong amplitude *and* wrong ramp start. Now `2026-08`.
   It stays **cycle-scoped**: repoint it every cycle, or pass `--headwind`.
2. It used a plain `rolling(28).mean()` instead of the canonical `display_ma` splice, reading 41K low at
   Aug-22. Now imports `display_ma` and reproduces the notebook exactly.
3. `DEFAULT_TARGET_DATE` moved 2026-08-22 → **2026-08-25** (the convention-independent trough minimum), and
   `TARGET_BULLSEYE`/`TARGET_TOL` were replaced by `TARGET_BAND = (45M, 46M)`.

It also now reports the trough argmin (a shape change can move it) and the seam derivative. Tests: 6
passing, including a new one that fails if the MA reverts to a plain rolling window.

Still true: it scores a **single date**, whereas the aim is arguably the whole seam → Oct-1 window. Scoring
the argmin covers most of that concern, but a window statistic is still worth considering.

One benign difference: the scorer clamps the ramp at `anchor_date` and the notebook does not. Under
seam-anchoring that only diverges after Dec-15, so trough and Dec-15 scoring agree.

### Reuse the cached BigQuery pull

```bash
--raw-cache-dir data-official/2026-08/desktop_baseline_2026-07-28/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825
```

Symlinks the already-fetched DAU aggregate so no probe re-queries BigQuery. ~15 min per probe; **3 in
parallel is proven fine** on this machine. Do not poll logs mid-run — wait for completion.

## 9. Traps

- **Never point `--results-dir` at `data-official/2026-08/desktop_baseline_2026-07-28/`.** That path *is*
  the canonical build, and a probe's output filename is byte-identical
  (`mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet`) — it would silently overwrite it. Probe output
  belongs under `research/param-scans/`.
- Give **every probe its own `--results-dir`**; slugs collide across rounds otherwise.
- `sps` and `seasonality_regime` require the sibling mozaic checkout on `configurable-model-params`
  (`126fe14`, `6f02912`, unpushed). The search is not reproducible without it.
- **zsh does not word-split unquoted variables.** Do not build a `$PARAMS` string and pass it unquoted —
  it arrives as a single argument and argparse rejects it. Write flags out explicitly. (This bit us once
  already.)
- Every artifact needs its `.raw.` / `.adj-{codes}.` marker plus a sidecar `.meta.json`; load through
  `mozaic_daily.adjustments.load_forecast`, never bare `pd.read_parquet`.
- **Leave `o` and `m` alone** during the search. Both are ~4–5 week-stale carry-forwards; changing one
  makes the trough delta unattributable to parameters.
- The Bash safety classifier has been intermittently unavailable in this project; manual approval mode
  may be needed.

## 10. Open elsewhere — not part of this search

- **Post-anchor ramp overshoot.** Three of four ramp implementations don't clamp at `anchor_date`;
  `score_near_horizon.py` does. Affects only 2026-12-16→31 and no reported number. Flagged, unfixed.
- **The Win10 anchor is unvalidated.** Attenuated four times (+175,000 cumulative, 12.3% of June's value)
  with no telemetry check. `research/headwinds/` has untracked in-progress validation output from another
  agent — not read, not committed.
- **`../aug22-retune/` blobs are archived to GCS**, not on disk. Pull from
  `gs://…/july-2026/param-scans/aug22-retune/` rather than re-running those rounds.
