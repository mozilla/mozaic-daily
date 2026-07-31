# `data-official/2026-08/` — August 2026 forecast cycle

Active cycle (branch `august-forecast`, off `clean-slate`).

## Status: desktop LOCKED to **g01** (LOL-200K, headwind −1,220,000) · mobile REBUILT on **`p`**

> **MOBILE METHODOLOGY CHANGED 2026-07-31.** The `m` marketing-lift overlay is retired; mobile now
> uses **`p` (`paid_organic_split`)** — mozaic forecasts **organic** DAU and marketing's paid curve
> is stacked on as a **level**. Mobile Dec-15 **17,864,732 → 17,601,155 (−263,577)**. Mobile
> parameters are unchanged; desktop is byte-identical (0 delta on all 365 curve days).
> Revert kit: `mobile_adjm_REVERT_2026-07-31/REVERT.md` — **three co-changed inputs, one unit.**
> Method: `organic/_index.md`. Evidence + reproduction: `../../research/mobile-organic/_index.md`.

> **s01 is preserved as a REVERT TARGET at `desktop_s01_REVERT_2026-07-29/`.** The 2026-07-30 swap
> to g01 changed the model config **and** the headwind anchor (+25,000) as one unit; reverting means
> undoing both. See that directory's `REVERT.md`. **Do not delete it while August is the live cycle.**

Current build is at forecast_start **2026-07-28** (trained through 2026-07-27). Five changes from
July — four desktop, one mobile (`m` → `p`, described above and in `organic/_index.md`):
the **desktop model retuned — first to s01, then on 2026-07-30 to g01** (`regime=multiplicative, cps=0.1649,
cpr=0.814, recent=17, ncp=40`; s01 was `cps=0.1849,
cpr=0.734, recent=17, ncp=35`), the **launch-on-login (`l`) ceiling raised 125,000 → 200,000 DAU/day**,
the **Win10 headwind desktop anchor attenuated +125,000 to −1,220,000**, and the **headwind ramp
re-anchored to start at the seam** instead of 2026-04-01. Mobile keeps July's PARAMETER lock but
changes its paid methodology. `o` is an unchanged stale carry-forward from July.

**The s01 retune is the largest change of the cycle**: summer trough **+1,359,887** for **+5,642** at
Dec-15 (11% of the ±50,000 budget). Canonical build: `desktop_locked/` (LOL 200K). Delta evidence:
`../../research/param-scans/summer-trough-v2/s01_canonical_desktop.ipynb`. Holiday parameters were
excluded from the search on principle — strictly local effects must not be used to move a whole-season
quantity — and the notebook asserts all four are at defaults.

**The headwind seam step is fixed; a smaller display artifact remains.** The original −564,262 drop was
100.9% the headwind switching on at 45.7% of its ramp. Re-anchoring removed it and lifted the near term
without moving Dec-15 at all — the ramp now contributes exactly 0 at the seam. See
`adjustments/_index.md` and `desktop_adjustment_ladder.ipynb`.

**The second, smaller seam artifact is also now FIXED (2026-07-29).** It was a **+102,595 upward** step
from `reconstruct_matched_daily`, which deseasonalized with a 7-day centered mean computed on the forecast
only — at the seam that degenerates to a weekday-only forward window (`min_periods=4`) and read ~10% high,
then got multiplied by the day-of-week factor on top. `Fix A` divides by the forecast's own day-of-week
profile *before* smoothing, so window composition stops mattering. The fixed implementation lives in
`src/mozaic_daily/seam_ma.py`; `data-official/2026-06/export_canonical_curves.py` is untouched so past
cycles cannot move, and code still bound to it is in `_archive/`.

On this build the residual display distortion at the seam is **+102 DAU** (was 211,480), and the curve now
steps **−107,445** — essentially the model's own plain 28d-MA step of −107,547. Dec-15 and everything from
seam+27 onward were byte-identical before and after the fix. The seam-kink figures were re-measured as a
result: s01 **−19,702/day vs −74,237** (pre-fix they read −20,604 vs −72,593). See
`research/ma-seam-turbulence/LOG.md` § Fix A, `seam_step_diagnosis.ipynb`, and
`seam_fix_before_after.ipynb` for the verification.

**Dec-15 2026 28d-MA (headwind applied):**

| platform | Aug current | Jul delivered | delta |
|---|--:|--:|--:|
| Desktop | 48,697,603 | 48,585,483 | +112,120 (+0.23%) |
| Mobile | **17,601,155** | 17,923,869 | **−322,714 (−1.80%)** |
| **ALL** | **66,298,758** | **66,509,352** | **−210,594 (−0.32%)** |

Mobile's move is the `m` → `p` methodology change, not a change of view on paid acquisition:
marketing's curve is unchanged and `p` consumes that very curve. `m`'s lift is identically zero
before its 2026-03-30 anchor, so Prophet was handed a training series that still contained all the
paid growth from 2024-06 on and extrapolated it as *organic* — then the add-back layered paid on
again. Total mobile DAU grew **+16.12%/yr** over the training window; organic grew **+11.60%/yr**.
The near term moves the other way (**+39,957** at Aug-25) because `p` restores the full measured
paid to recent training rows; it is the slope that flattens.

Superseded mobile values, for reading older text in this file: `m` UAC+Meta **17,864,732**,
`m` July-carried-forward **17,924,607**, and ALL **66,562,335** / **66,622,210** respectively.

**Aug-25 trough minimum** (28d-MA, post-headwind) — the scored near-horizon KPI: Desktop
**45,041,389**. Aug-22 for continuity with earlier builds: Desktop **45,091,364** · Mobile 17,056,561 ·
ALL 62,147,926.

Note Aug-22 moved by −4,443 when `Fix A` landed, because it sits *inside* the 27-day seam transition and
so was never covered by the far-horizon guarantee. Aug-25 sits a full window past the seam and did not
move — one more reason to prefer it for anything quotable.

Aug-25 is scored rather than Aug-22 because it is exactly 28 days past the seam, so its window is
entirely forecast and its value is independent of the `display_ma` splice convention; Aug-22 sits inside
the transition zone and reads ~41K apart under the two conventions.

### Attribution ledger

Six changes separate July's delivered number from this build, and **all six are attributed**:

| desktop Dec-15 28d-MA | step | running |
|---|--:|--:|
| July delivered (125K LOL, hw −1,345,000, 07-06 anchor) | — | 48,585,483 |
| + data refresh to the 07-28 anchor | −64,769 | 48,520,714 |
| + LOL ceiling 125,000 → 200,000 | +77,604 | 48,598,318 |
| + Win10 headwind −1,345,000 → −1,220,000 (both attenuations) | +125,000 | 48,723,318 |
| + desktop model retune to s01 | +5,642 | 48,728,960 |
| **+ desktop model retune s01 → g01 (measured, config-isolated)** | **−31,357** | **48,697,603** |

The headwind line now carries **both** attenuations (−1,345,000 → −1,245,000 → −1,220,000). The final
+25,000 belongs with the g01 retune as one decision — it was applied to absorb most of that step's
−31,357 — and appears on the headwind line only because `h` and the model config are separate
mechanisms. The notebook asserts the chain closes; it currently closes to **−0**.

⚠️ **The LOL step was two steps until 2026-07-30.** The ceiling was raised in stages
(125,000 → 180,000 = +52,256, then 180,000 → 200,000 = +25,348), each differenced against its own frozen
build. The intermediate curves and the `desktop_superseded_lol180k_2026-07-28/` comparison build were
deleted at the user's instruction, so the two steps are **merged into one +77,604 line** here. The total
is unchanged and the chain still closes.

**The split is still reproducible, just not from `data-official/`.** The deleted 180K build was the same
run as `research/param-scans/summer-trough-v2/s01_gradient/cps0.1849_thresh032_recent17_cpr0.734_ncp35_clip0.6_sps0.00825_regimemultiplicative/`
— verified identical sidecar (same `model_config`, same `adjustments_applied` incl. the `l` spec sha1
`e23a6267`, same commit) and the two directories' `.pkl` files were hard links to one inode. So
differencing that parquet against `desktop_locked/` still yields +25,348, and against
`desktop_baseline_2026-07-28/` still yields the +5,642 retune. What *is* gone is the ability to rebuild
any of it from scratch, since the intermediate curves no longer exist.

**Each remaining step differences two builds that differ in exactly one input** — except the merged LOL
step, which spans two raises — and each is *pinned* as a constant in `[desktop-dec15]` rather than
recomputed. Two consequences:

- **The ledger is a real check, not a tautology.** Because the steps are pinned independently of the
  canonical parquet, they do not close by construction — so the notebook asserts the chain sums to the
  measured Dec-15 and fails loudly if a constant goes stale or the wrong parquet is loaded. It currently
  closes to a residual of **−0**. A residual ledger cannot test itself.
- **No comparison build was re-run to produce any of these numbers.** **Historical builds are locked** —
  published deltas were quoted against them, and a chain whose links move cannot be audited. The
  intermediate parquets are also gitignored and GCS-bound, which is the second reason the steps are pinned
  rather than loaded.

The LOL step is **+77,604** against a raw curve change of +75,000/day at Dec-15 — a 103% pass-through,
which the notebook asserts falls in a plausible 0.5–1.5× band. Near-zero would mean the bidirectional
add-back leg never ran; far above 1 would mean the training subtraction is reshaping trend.

**The retune's own like-for-like measurement** is in
`research/param-scans/summer-trough-v2/s01_canonical_desktop.ipynb`: previous config vs s01 with the
**LOL curve held identical on both sides**, so the model config is the only difference — trough
**+1,359,887** for **+5,642** at Dec-15, 11% of the ±50,000 budget. Both sides of that comparison sit
on a curve that is no longer this cycle's active one, which is *why* it isolates the config; do not
repoint it at the canonical build, and note that its 45,193,561 trough is therefore not this build's
(s01 troughed at 45,223,249; g01 troughs at 45,041,389).

**A ceiling change is not a level shift.** `l` is bidirectional, so a higher ceiling also changes the
training series Prophet fits, and its realised Dec-15 effect depends on the model config — the same
20,000/day raise measured +25,348 under s01 but only ~+7,211 under the previous flatter config. That
config-dependence is why the merged +77,604 cannot be re-derived by scaling the curve delta, and why a
future ceiling change needs a fresh measurement rather than a pro-rata estimate.


**The headwind amplitude step is exactly +100,000 by construction.** `h` is applied to the 28-day MA,
never to the training frame, so its Dec-15 effect is the anchor delta with no Prophet interaction and it
needs no model re-run.

**The ramp re-anchoring is absent from the ledger because its Dec-15 effect is exactly zero** — both
conventions terminate on the same anchor. It is not a KPI change; it is a near-horizon reshaping
(+569,419 at the seam, +467,737 at Aug-22, +305,046 at Oct-1) that also removed the seam discontinuity.
Near-horizon numbers from this build are therefore **not** comparable to the earlier August builds; Dec-15
is.

### Read this before quoting the headline

**Both discretionary levers moved upward this cycle and together they set the sign of the result:**

| lever | change | Dec-15 effect | basis |
|---|---|--:|---|
| `l` LOL ceiling | 125,000 → 200,000/day | **+77,604** | extrapolation judgement; unfalsifiable |
| `h` Win10 anchor | −1,345,000 → −1,220,000 | **+125,000** | calibration judgement; last +25,000 is part of the g01 swap |
| `h` ramp start | 2026-04-01 → seam | **0** | convention correction (measured) |
| data refresh | 07-06 → 07-28 anchor | −64,769 | what the fresher data said |
| s01 model retune | previous config → s01 | +5,642 | measured, config-isolated |
| **g01 model retune** | s01 → g01 | **−31,357** | measured, config-isolated; buys −186,860 at Aug-25 |

**The two judgement calls account for more than the entire headline gain.** Data and model together
came to **−59,127** (−64,769 refresh, +5,642 retune); the two discretionary levers add **+177,604**.
Net **+118,477**. So August's improvement versus July is not "what the data now says" — the data said
down. It is two upward judgements, neither of which currently has a validation artifact.

That does not make either wrong — July's 125K LOL clamp sat *below* the last clean measurement of
130,296, so it was arguably too conservative — but the framing matters when this number is quoted, and
it matters more now that the intermediate LOL variants have been deleted: the single remaining curve
should not be mistaken for a measured or consensus value.

**Intermediate-variant history was removed on 2026-07-30.** Earlier in the cycle this section carried a
table of every LOL-ceiling / headwind-anchor / ramp-start combination built at the 2026-07-28 anchor
(five rows on the previous model parameters, plus the s01 pair). It was deleted along with the
intermediate curves, because a standing menu of superseded ceilings invited exactly the
over-referencing this cleanup was meant to stop.

**There are two desktop builds for this cycle:** `desktop_locked/` — **g01** parameters, LOL 200,000,
headwind −1,220,000 ramping from the seam (**Dec-15 48,697,603, Aug-25 trough 45,041,389**) — and
`desktop_s01_REVERT_2026-07-29/`, the complete s01 build kept as a **revert target**, not an archive. The
ledger above is now the only record of the path taken to it; the intermediate builds' own numbers are
in git history.

The one path-dependent fact worth keeping out of history: **the ramp re-anchoring changed the trough,
not Dec-15.** Same anchor either way, so Dec-15 is identical, but the Aug-22 trough moved +467,737 and
the seam discontinuity disappeared. Near-horizon numbers from this build are therefore not comparable
to any pre-2026-07-29 August figure.

**Still not the number to publish.** `o` and `m` remain ~4–5 weeks stale; the headwind anchor has been
attenuated four times running without data-side validation; the LOL ceiling is unfalsifiable against
current data (measurement stopped 2026-06-23). See the caveats cell and `adjustments/_index.md`.

## Current working set

- **Producer / review notebook** — `august_canonical_v2026-07-28.ipynb` (25 cells, executed with
  outputs). The single canonical view: both platform plots, the ex-Iran mobile plot, the Dec-15 table,
  and the caveats. All plots are generated inside the notebook and saved to `plots/`.
  A **"Dec-15 vs the low / baseline / stretch benchmarks"** section (`[desktop-vs-targets]`,
  `[mobile-vs-targets]`) prints absolute Dec-15 values and per-benchmark deltas for each platform.
  Both cells label the benchmarks as the **June-cycle aspirational markers reused in July, not August
  targets** (see the `target`-column note below) — August desktop lands below all three, mobile above
  all three. Values are computed from the curves, not hardcoded, so they follow a rebuild.
  Also carries four **`*_with_2025` reference charts** overlaying 2025 actuals as a faint calendar-aligned
  grey line, built from parquet `training` rows rather than a BQ query (verified equal to actuals by
  `scripts/verify_training_rows_are_actuals.py`). Visual reference only — they feed no number. See
  `plots/_index.md` for why they are separate charts rather than edits to the four originals.
  Its final `[csv-export]` cell writes the two public CSVs in `csv/` and round-trips them: both files
  are re-read from disk and their Dec-15 values re-checked against the in-memory curves, and the
  forecast columns are asserted to begin exactly at the seam.
- **Public CSV exports** — `csv/august_canonical_curves.csv` (365 × 10, full-year daily 28d-MA for
  desktop/mobile/ALL × actuals/prior-July/current-August) and `csv/august_dec15_summary.csv` (3 rows:
  Dec-15 headline + summer trough per platform). Both git-tracked via explicit `.gitignore` exceptions.
  Column reference, what's baked in, and provenance: `csv/README.md`. **No `target` column this cycle** —
  July had a stakeholder desktop target of 48,584,362 but no August target has been set, and carrying
  July's forward under that name would read as an August target. The `summer_trough_*` columns are new:
  the trough is the KPI the s01 retune was adopted to move, so a Dec-15-only summary would omit the
  cycle's largest change.
- **Stakeholder handoff bundle** — `handoff/august_canonical_handoff_2026-07-28.zip` (~1.7 MB): the two
  CSVs, a boss-facing `README.md` with a hand-verification checklist (checkpoint values, expected seam
  step, boundary row counts), the 9 canonical charts, and 5 adjustment-ladder / seam-fix diagnostics.
  See `handoff/_index.md`. The zip and its staging dir are gitignored; regenerate rather than duplicate.
- **Adjustment-ladder diagnostic** — `desktop_adjustment_ladder.ipynb` (+ `adjustment_isolation/`).
  Desktop only. Turns each adjustment on one at a time so its individual effect is visible. **Its main
  finding: the seam discontinuity at 2026-07-28 is 100.9% the display-layer headwind**, not the model —
  the raw model output is continuous across the seam, and no parameter value can change that. Also
  measures the `l`/`o` interaction term (−15,590 at Dec-15) and contrasts the two headwind ramp
  conventions. **Read this before starting a parameter search.**
- **Desktop forecast (CANONICAL, and the only one)** —
  `desktop_locked/mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet` — g01 params, `l` at the
  **200K** ceiling + `o`. Pre-headwind.
- **Desktop forecast (previous-cycle params, superseded)** — `desktop_baseline_2026-07-28/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet` — kept for two reasons: its raw BQ pull is the shared cache every scan and isolation run symlinks, and its Dec-15 (48,672,970) is the anchor point the ledger's retune step is pinned from. The canonical notebook **no longer loads it** — all ledger steps are pinned constants
  (+ sidecar, `parameters.json`). Pre-headwind; `l` + `o` baked in. **FROZEN — never re-run or rebuilt.**
  It was built on a superseded LOL curve that no longer exists on disk, which is another reason never to
  attempt a rebuild; the frozen artifact is the record. **The directory name is a misnomer** — it held
  the original baseline, which later runs overwrote in place. Kept as-is because the notebook and the
  committed sidecar reference the path.
- **Mobile forecast** — `mobile_organic_2026-07-28/cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1/mozaic_daily_forecast.2026-07-28.gm-D.adj-p.parquet`
  Two superseded `m`-era builds remain on disk and must not be deleted while August is live:
  `mobile_uac_meta_2026-07-28/` (the immediate predecessor, 17,864,732 — its parquet AND its
  838 MB pickle are the revert target) and `mobile_baseline_2026-07-28/` (17,924,607).
  (+ sidecar, `parameters.json`). Pre-headwind; `m` baked in. **Not rebuilt for the LOL change** — `l`
  is desktop-only, so this is byte-identical to the baseline run and its Dec-15 is unchanged (the
  notebook asserts the drift is exactly 0).
- **Adjustment specs (wired)** — `adjustments/headwind.json` (`h`, display layer),
  `launch_on_login/lol.json` (`l`), `mozillaonline/mozillaonline.json` (`o`),
  `marketing/marketing.json` (`m`). `o` and `m` are byte-identical carry-forwards of July's with only
  `applies_to_forecast_start` moved 2026-07-06 → 2026-07-28. **`l` is rebuilt** (200K ceiling — see
  `launch_on_login/_index.md`) and **`h` has changed twice** (desktop amplitude −1,345,000 → −1,245,000,
  and `start_date` 2026-04-01 → 2026-07-28 — see `adjustments/_index.md`).
- **Iran** — queried natively; the shutdown gap is covered by mozaic's built-in counterfactual fill
  (auto-applied by `populate_tiles`). No cycle-local artifact needed.

## How the current build was produced

The **mobile** command below was run once (2026-07-29) and has not been re-run since — `l` is
desktop-only, and mobile's headwind *amplitude* never moved (its ramp start did, but `h` is display-layer
so that needs no model run). The **desktop** command was run repeatedly into the same directory, each run
overwriting the last, as the LOL ceiling was raised to its final 200,000. Runs after the first reused the
cached raw BQ pull already in the slug dir, so no `--raw-cache-dir` was needed and no BigQuery re-query
happened.

⚠️ **The per-ceiling run logs were deleted on 2026-07-30** along with the intermediate curves, so only
`logs/aug_baseline_{desktop,mobile}_2026-07-28.log` (the first desktop run) survives. The commands below
are the reproducible record; the intermediate invocations differed only in which curve `lol.json` named.
Note that the *canonical* build lives in `desktop_locked/`, not in the `desktop_baseline_*` directory
these commands write to — see that directory's `README.md` for the final invocation.

**Headwind changes need none of this.** `h` is display-layer, so the two anchor steps (−1,345,000 →
−1,295,000 → −1,245,000) were spec edits plus a notebook re-execution, with no model rebuild.


```bash
source .venv/bin/activate

python scripts/run_param_scan.py --forecast-start-date 2026-07-28 \
    --results-dir data-official/2026-08/desktop_baseline_2026-07-28 \
    --changepoint-prior-scale 0.08983 --changepoint-range 0.65 --n-changepoints 25 \
    --recent-weeks 13 --holiday-threshold -0.032 --holiday-max-radius 5 \
    --holiday-min-radius 3 --holiday-effect-floor -0.6

python scripts/run_mobile_param_scan.py --forecast-start-date 2026-07-28 \
    --results-dir data-official/2026-08/mobile_organic_2026-07-28 \
    --changepoint-prior-scale 0.035 --changepoint-range 0.75 --n-changepoints 25 \
    --recent-weeks 13 --holiday-threshold -0.055 --holiday-effect-floor -0.6
```

`run_main.py` **cannot** reproduce these — it has no parameter flags and would use package defaults.
The two param-scan runners are the real producers, and they apply the overlays whose spec's
`applies_to_forecast_start` matches the run date. Logs: `logs/aug_baseline_{desktop,mobile}_2026-07-28.log`,
and `logs/aug_mobile_organic_2026-07-28.log` for the `p` rebuild. **The mobile command above now
produces `.adj-p.`, not `.adj-m.`** — the runner derives the code from whichever spec gates the run,
and `marketing.json`'s date gate is cleared. Both scan runners now pass their config through
`main(model_configs=...)` instead of monkeypatching `process_data_source`.

**The date gate is the trap.** Overlay specs are matched by exact string equality on
`applies_to_forecast_start`. A run at a date no spec claims applies **no** overlays and silently emits
`.raw.` instead of `.adj-lo` / `.adj-m`. The notebook defends against this: `load_all_level_dau` passes
`require_state=["l","o"]` / `["m"]`, so a mis-gated run fails loudly at load instead of producing a
plausible wrong headline.

## Verification built into the notebook

Three checks run as assertions, not eyeballs:

1. **Config lock** — desktop's sidecar is compared field-by-field against the **g01** lock (and its four
   holiday knobs against package defaults, enforcing the exclusion policy); mobile's against July's locked
   values (8 params each). Any drift aborts.
2. **State markers** — `load_forecast(..., require_state=...)` pins which adjustments must be present.
3. **Prior-curve reproduction** — July's delivered Dec-15 numbers (48,585,483 / 17,923,869) are
   hardcoded and the rebuilt prior curve must match within 1,000 DAU. Both reproduce at **drift 0**,
   which is what licenses quoting the August-vs-July deltas at all.

## Expected layout (populate as the cycle progresses)

```
2026-08/
  august_canonical_v<date>.ipynb   # producer/review notebook (present)
  desktop_locked/                  # present — CANONICAL (g01, LOL 200K)
  desktop_s01_REVERT_2026-07-29/   # present — s01 REVERT TARGET (config + headwind); see its REVERT.md
  desktop_baseline_2026-07-28/     # present — superseded (July's params); ledger baseline + raw cache
  mobile_organic_2026-07-28/       # CANONICAL mobile (adj-p)
  mobile_uac_meta_2026-07-28/      # superseded adj-m — REVERT TARGET, do not delete
  mobile_adjm_REVERT_2026-07-31/   # revert kit for the m -> p swap
  organic/                         # `p` spec + measured paid/organic split
  mobile_baseline_2026-07-28/      # present
  adjustments/headwind.json        # present (h)
  launch_on_login/lol.json         # present (l)
  mozillaonline/mozillaonline.json # present (o)
  marketing/marketing.json         # present (m)
  plots/                           # present
  csv/august_canonical_curves.csv  # present — + august_dec15_summary.csv, README.md
  handoff/                         # present — _index.md + the gitignored bundle zip
  kpi_sheet/                       # NOT YET
  TODO_factors.md                  # NOT YET — start it as a diff against ../2026-07/TODO_factors.md
```

The CSV export was deferred earlier in the cycle on the grounds that the build was baseline-only. It
is now cut, but **the "still not the number to publish" caveats above have not been resolved** — `o`
and `m` remain stale and the headwind anchor is still unvalidated. Both the CSV README and the handoff
bundle README state this prominently, so the export is a snapshot of the current build rather than a
sign-off. Re-cut both when the overlays land.

## Next up

- **Re-measure and swap the remaining two overlay curves** (`o` MozillaOnline, `m` marketing). Both are
  ~4–5 week-stale carry-forwards and each needs a fresh build against data through late July. This is
  now the main reason the forecast is not deliverable. `l` is **rebuilt** (200K, 2026-07-29) and is now
  the cycle's only LOL curve, though its ceiling remains an unfalsifiable judgement — deleting the
  alternates removed the menu, not the uncertainty.
  Do them one at a time: changing two overlays in one run makes the Dec-15 delta uninterpretable.
- **Validate the Win10 headwind anchor against data.** Now at −1,220,000, attenuated +125,000 from July's
  −1,345,000 in two steps on 2026-07-29. That is the fourth successive attenuation on the same rationale
  (−1,420,000 → −1,370,000 → −1,345,000 → −1,295,000 → −1,245,000 → −1,220,000), and none has been checked against a
  held-out estimate of how much of the Win10 decline Prophet actually absorbed. They are calibrated
  judgements, each of which raises the reported number. A validation pass against realised Win10-cohort
  DAU is worth doing before any further step. See `adjustments/_index.md`.
- **`TODO_factors.md`** — begin as a diff against July's.
- **Open, needs go/no-go — summer-trough overlay.** `research/param-scans/aug22-retune/` established
  that no exposed parameter combination lifts the Aug trough to target while holding Dec-15 (best
  sampled point 0.385M short; `seasonality_regime=multiplicative` gets ~71% of the lift but plateaus).
  Its recommendation is a bidirectional overlay in the `l`/`o`/`m` family tapering to ~0 by Nov/Dec.
  **Nothing here implements it and nothing was tuned toward the trough.** Target shape:
  `research/summer-slump/`.
- **`../2026-06/` is retained on purpose** even though it is N-2. This notebook still imports
  `display_ma` from `../2026-06/export_canonical_curves.py`, and July's `m` chain reaches into it. The
  September roll-forward should give August its own copy. See `../_index.md`.

## Dependency note

The `seasonality_prior_scale` / `seasonality_regime` knobs now appear in every config slug
(`_sps0.00825` / `_sps0.1`) because they were exposed in `mozaic-forecasting-official` @
`configurable-model-params` (`126fe14`, `6f02912`) after July's build. **Their defaults reproduce the
values July hardcoded**, so the baseline is behaviour-comparable to July despite the slug change. Those
commits are **not pushed** to that repo's origin — reproducing this needs the local checkout.

## Where new files go

Month-scoped artifacts (this cycle's producer/diagnostic notebooks, adjustment specs, parquets,
canonical CSVs) live here. Cross-month or topic-anchored work (mechanism diagnostics, parameter
searches, validation against actuals over time) goes to `research/{topic}/`.
