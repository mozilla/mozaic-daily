# Handoff: fix the seam step in `reconstruct_matched_daily`

The published desktop curve steps **+102,595 upward** at the seam. This is a display artifact with a
located cause, not a model property and not a return of the headwind step. The job is to fix it without
regressing the day-27 splice — which is exactly the trade the June cycle already lost once.

Read `LOG.md` in this directory before proposing anything. It records the fix you will think of first
being tried and rejected.

---

## 1. What is settled — do not re-derive

All three measured to the dollar (`diagnose_recon_edge_bias.py`, plus the eliminations below):

| candidate cause | contribution at the seam |
|---|--:|
| Win10 headwind (`h`) | **exactly 0** — the seam re-anchoring works |
| parquet training rows vs BQ actuals | **−0/day** (max ±3 DAU, rounding) |
| `continuous_splice` cubic correction | **exactly 0** |

**The old −564,262 downward step was 100.9% headwind and is genuinely fixed.** Do not reopen it.

## 2. The actual cause

`reconstruct_matched_daily` (`data-official/2026-06/export_canonical_curves.py`, ~line 146)
deseasonalizes the forecast with:

```python
trend_fc = fc.rolling(7, center=True, min_periods=4).mean()
```

computed on the **forecast only**. At the seam that centered window has no left half inside `fc`, and
`min_periods=4` silently accepts the first four forecast days. 2026-07-28 is a Tuesday, so those four days
are **Tue/Wed/Thu/Fri — all weekdays, no weekend.** For a 7-day deseasonalizing window that is the
maximally biased sample.

Desktop's weekday/weekend swing is ~1.5–1.6×, so the damage is large:

| build | `trend_fc[seam]` as-is | DoW-complete forward-7 | bias |
|---|--:|--:|--:|
| s01 (canonical) | 51,218,288 | 45,951,786 | **+5,266,502 (10.3%)** |
| superseded | 48,835,094 | 44,633,409 | **+4,201,685 (8.6%)** |

After the Tuesday day-of-week factor the first reconstructed day lands ~5.9M above raw, which is
**+211,479 on a 28-day MA** (s01) / +206,658 (superseded) — matching the observed step contribution
exactly. The bias decays over the first ~3 days (+5.79M, +2.99M, +0.29M, −0.54M on the superseded build);
by seam+3 the centered window is complete.

The docstring asserts this edge case is safe — *"center=True is safe at the seam's left edge... always has
>= min_periods=4 forward points"*. That is the defect: having 4 points satisfies `min_periods`, but four
consecutive weekdays is not a 7-day trend. **Fix the docstring too.**

### Why it surfaced now, and a correction to the record

The bias was always ~+207K. It was buried inside the headwind's −569,419 step; removing that exposed it.

**The superseded build's apparent continuity (+5,157) was a coincidence, not continuity:**

| build | plain-MA step | reconstruction bias | net |
|---|--:|--:|--:|
| superseded | −201,501 | +206,658 | **+5,157** |
| s01 | −108,884 | +211,479 | **+102,595** |

The superseded build's first forecast day is anomalously low (−2.89M week-over-week, same weekday), which
happened to cancel the bias. s01's first day is plausible (−0.29M WoW), so the bias shows through. Any
claim that "the model is continuous across the seam" rests on that cancellation and should not be repeated.

## 3. Mobile has the same bug, ~28× smaller

| platform | `trend_fc[seam]` bias | MA effect | seam step | as % of level |
|---|--:|--:|--:|--:|
| desktop (s01) | +5,266,502 | +211,479 | +102,595 | +0.22% |
| mobile | +184,709 | +7,619 | −1,498 | −0.009% |

Same mechanism; mobile's weekly amplitude is much smaller relative to level. **Any fix changes both
platforms' published curves**, so both must be verified.

## 4. The fix you will think of first was already rejected

Centering `trend_fc` on the concatenated `pre + fc` series. `LOG.md`:

> A seam-aware centered trend (computing `trend_fc` on the concatenated pre+fc series) was tested and
> **rejected** — it *worsened* the global splice (ALL 0.086% → 0.698%) while only helping BR/IN, a net
> loss for the headline curve.

Reproduced 2026-07-29: it flips the step to **−149,405** (superseded) / **−93,726** (s01). So it trades a
day-0 upward step for a day-0 downward step *and* a worse day-27 handoff. This is a genuine tension
between **level continuity at day 0** and **smoothness at day 27**; June chose day 27.

## 5. The hypothesis to test first

**Keep the estimator forecast-only, but make its window day-of-week complete.** The defect is not "uses
forecast only" — it is "uses 4 weekdays". A **forward 7-day** window at the incomplete edge (indices 0, 1,
2) is still forecast-only, so it should not touch the day-27 splice that killed June's attempt, and it is
DoW-balanced by construction.

Cost: it shifts the trend estimate's effective center forward by up to 3 days. At the observed decline of
~50–130K/day on the MA that is a ~150–400K level error on the *daily* value — an order of magnitude below
the 4.2–5.3M weekday bias it removes.

Concretely: `min_periods=7` plus a forward-window fallback for the first 3 rows, or equivalently a
one-sided 7-day mean where the centered window is incomplete. Verify it is genuinely DoW-complete for a
seam falling on **any** weekday, not just Tuesday.

## 6. A second instance of the same bug, in the same function

The day-of-week profile is estimated with:

```python
recent_trend = recent.rolling(7, center=True, min_periods=4).mean()
```

on the *actuals*, over a 13-week window — so both edges of that window get partial-window trend estimates,
and `ratio = recent / recent_trend` feeds `dow_act`. NaNs are dropped, but partial-window rows are not:
they produce biased ratios that enter the profile. **Not yet quantified — check it.** `dow_act` is
normalized to mean 1, so the level effect may largely cancel, but the *shape* may not.

`LOG.md` also records "no winsorization of the 13-week DOW estimate in v1" as a known limitation.

## 7. Acceptance criteria

A fix must satisfy all of these:

1. **Aug-25 and Dec-15 byte-identical.** The corruption spans ~3 days; the headline numbers must not move.
   Current: s01 trough 45,193,561 / Dec-15 48,678,612; superseded 43,833,674 / 48,672,970.
2. **|seam step| reduced** on desktop, and not made worse on mobile.
3. **The day-27 splice not worsened** — this is June's rejection criterion and the one that matters most.
   Quantify the ALL corner metric; June's baseline is 0.086%, and the rejected fix took it to 0.698%.
4. **June and July delivered numbers still reproduce.** `display_ma` is shared across all three cycles'
   canonical notebooks, which assert their delivered values: July desktop 48,585,483 / mobile 17,923,869;
   June 46,893,112 / 47,834,362 / 16,911,773 / 17,511,100 (from `LOG.md`).
5. **Tests pass, and the tolerance gets tightened.**
   `tests/test_export_canonical_curves.py::test_seam_anchor_is_continuous_with_actuals` asserts
   `step/day1 < 0.02`. That 2% band is why nothing failed — s01's 0.22% is well inside it, but the band
   was calibrated when the step was +5,157. **Tighten it after fixing**, or the next regression hides too.
6. `backtest_seam.py` global gate passes; `verify_fix.py` shows Dec-15 delta 0.
7. Re-run and re-execute: `data-official/2026-08/august_canonical_v2026-07-28.ipynb` and
   `research/param-scans/summer-trough-v2/s01_canonical_desktop.ipynb` (the latter is the user's delta
   evidence — keep it working).

## 8. Files

| path | role |
|---|---|
| `data-official/2026-06/export_canonical_curves.py` | `reconstruct_matched_daily` (~146), `display_ma` (~193). **The thing to change.** |
| `tests/test_export_canonical_curves.py` | 7 tests, incl. the 2% seam tolerance |
| `research/ma-seam-turbulence/LOG.md` | **Read first** — records the rejected fix and the known limitations |
| `research/ma-seam-turbulence/diagnose_recon_edge_bias.py` | Per-day reconstructed-vs-raw table + the edge-bias measurement |
| `research/ma-seam-turbulence/{backtest_seam,verify_fix,diagnose_seam,build_report}.py` | June's harness — reuse rather than rewrite |

**Consumers of `display_ma`** (all must keep working): the June/July/August canonical notebooks,
`scripts/mobile_sensitivity.py`, `scripts/score_near_horizon.py`,
`research/param-scans/aug22-retune/export_bestfit_curve.py`.

## 9. Traps

- **`display_ma` lives under `data-official/2026-06/`** and August still imports it from there. It is a
  cross-cycle dependency already flagged for the September roll-forward. A change here moves June's and
  July's curves too — hence acceptance criterion 4.
- **The 2% test tolerance masks this entire class of bug.** Do not leave it at 2%.
- The seam falls on a **Tuesday** this cycle. A fix that happens to work for Tuesday is not a fix — test
  a seam on each weekday.
- Desktop's weekend is ~1.5–1.6× below weekday. Any partial window that is not DoW-complete will be badly
  biased; check window completeness explicitly rather than trusting `min_periods`.
- **`min_periods` is not a completeness guarantee.** That assumption is what caused this.
- zsh does not word-split unquoted variables — write CLI flags out explicitly.
- `nb_cells.py` / `bq_query.py` by absolute path, always `--file`, never heredoc pipes.
- Legacy telemetry only, never Glean.

## 10. Out of scope

- The Win10 headwind seam step (fixed) and its anchor validation (`research/headwinds/`, another agent's
  untracked work in progress — leave it alone).
- The s01 lock itself. It is canonical as of `82bdb96`; this fix must not change its headline numbers.
- The `o` / `m` stale overlay refreshes.
- Whether the summer trough should be lifted further — that search is paused, not settled.
