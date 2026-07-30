# `data-official/2026-08/` — August 2026 forecast cycle

Active cycle (branch `august-forecast`, off `clean-slate`).

## Status: desktop LOCKED to s01, LOL-**200K**, headwind −1,245,000, seam-anchored ramp

Current build is at forecast_start **2026-07-28** (trained through 2026-07-27). Four changes from July:
the **desktop model retuned to the s01 config** (locked 2026-07-29 — `regime=multiplicative, cps=0.1849,
cpr=0.734, recent=17, ncp=35`), the **launch-on-login (`l`) ceiling raised 125,000 → 200,000 DAU/day**,
the **Win10 headwind desktop anchor attenuated +100,000 to −1,245,000**, and the **headwind ramp
re-anchored to start at the seam** instead of 2026-04-01. Mobile still carries July's lock. `o` and `m`
are unchanged stale carry-forwards from July.

**The s01 retune is the largest change of the cycle**: summer trough **+1,359,887** for **+5,642** at
Dec-15 on the 180K curve (11% of the ±50,000 budget). Canonical build: `desktop_locked/` (LOL 200K). Delta evidence:
`../../research/param-scans/summer-trough-v2/s01_canonical_desktop.ipynb`. Holiday parameters were
excluded from the search on principle — strictly local effects must not be used to move a whole-season
quantity — and the notebook asserts all four are at defaults.

**The headwind seam step is fixed; a smaller display artifact remains.** The original −564,262 drop was
100.9% the headwind switching on at 45.7% of its ramp. Re-anchoring removed it and lifted the near term
without moving Dec-15 at all — the ramp now contributes exactly 0 at the seam. See
`adjustments/_index.md` and `desktop_adjustment_ladder.ipynb`.

What remains is a **+102,595 upward** step, and it is a *different* artifact:
`reconstruct_matched_daily` deseasonalizes with a 7-day centered mean computed on the forecast only, which
at the seam degenerates to a weekday-only forward window (`min_periods=4`) and reads ~3.8M high. The
superseded build's apparent continuity (+5,157) was a coincidental cancellation against its anomalously
low first forecast day, not genuine continuity. **Headline numbers are unaffected** (the corruption spans
~3 days; Aug-25 and Dec-15 are identical under the candidate fix), but the seam-kink magnitude is
contaminated. The obvious fix was already tried and rejected in June — it worsens the day-27 splice. See
`research/ma-seam-turbulence/diagnose_recon_edge_bias.py`.

**Dec-15 2026 28d-MA (headwind applied):**

| platform | Aug current | Jul delivered | delta |
|---|--:|--:|--:|
| Desktop | 48,678,612 | 48,585,483 | +93,129 (+0.19%) |
| Mobile | 17,924,607 | 17,923,869 | +738 (+0.00%) |
| **ALL** | **66,603,219** | **66,509,352** | **+93,867 (+0.14%)** |

**Aug-25 trough minimum** (28d-MA, post-headwind) — the scored near-horizon KPI: Desktop
**45,193,561**. Aug-22 for continuity with earlier builds: Desktop **45,238,336** · Mobile 17,056,672 ·
ALL 62,295,008.

Aug-25 is scored rather than Aug-22 because it is exactly 28 days past the seam, so its window is
entirely forecast and its value is independent of the `display_ma` splice convention; Aug-22 sits inside
the transition zone and reads ~41K apart under the two conventions.

### Attribution ledger

| desktop Dec-15 28d-MA | step | running |
|---|--:|--:|
| July delivered (125K LOL, hw −1,345,000, 07-06 anchor) | — | 48,585,483 |
| + data refresh to the 07-28 anchor | −64,769 | 48,520,714 |
| + Win10 headwind −1,345,000 → −1,245,000 | +100,000 | 48,620,714 |
| **+ LOL ceiling 125,000 → 200,000 AND the s01 retune (combined residual)** | **+83,246** | **48,703,960** |

**The LOL and retune steps are deliberately NOT separated.** Isolating the retune needs a build
identical to the canonical one in every input except the model config. One exists at the 180K ceiling
(the frozen `desktop_baseline_2026-07-28/` comparison point) but none exists at 200K, and **historical
builds are locked — they are never re-run, rebuilt, or replaced to manufacture one.** Published deltas
were quoted against those artifacts; a chain whose links move cannot be audited. So the canonical
notebook reports one combined step and says so, rather than a clean-looking number obtained by
regenerating a historical comparison point.

The combined step is a residual, so the ledger sums by construction and is not itself a check. What the
notebook *does* assert is that the residual is physically plausible: the LOL curve is +75,000/day higher
than July's 125K curve at Dec-15 and the retune carried a ±50,000 budget, so the step must be positive
and below +125,000. Realised **+83,246** = 111% of the curve change. Near-zero would mean the add-back
leg never ran; far above the envelope would mean the training subtraction is reshaping the trend
unexpectedly.

**The retune's own like-for-like measurement** lives in
`research/param-scans/summer-trough-v2/s01_canonical_desktop.ipynb`, which compares s01 against the
previous config on an identical 180K curve: trough **+1,359,887** for **+5,642** at Dec-15. Quote the
retune effect from there. That notebook stays on 180K builds on purpose — repointing it at 200K would
confound the config evidence with the ceiling change.

**Note the ceiling's effect is config-dependent.** 180K → 200K is worth +25,348 at Dec-15 under s01 but
only ~+7,211 under the previous, flatter config: `l` is bidirectional, so a higher ceiling changes the
training series too, and s01's 2.1× changepoint flexibility responds far more. Never treat a ceiling
change as a level shift.


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
| `l` LOL ceiling | 125,000 → 200,000/day | (combined with the retune: +83,246) | extrapolation judgement; unfalsifiable |
| `h` Win10 anchor | −1,345,000 → −1,245,000 | +100,000 | calibration judgement |
| `h` ramp start | 2026-04-01 → seam | **0** | convention correction (measured) |
| data refresh | 07-06 → 07-28 anchor | −64,769 | what the fresher data said |

The data component alone pointed **down** −64,769; the two judgement calls add +152,256. So August's
+87,487 versus July is not "what the data now says" — it is the model plus two upward judgements, neither
of which currently has a validation artifact. That does not make either wrong (July's 125K LOL clamp sat
*below* the last clean measurement of 130,296, so it was arguably too conservative), but the framing
matters when this number is quoted.

**Variant history this cycle** — all at the 2026-07-28 anchor, same data:

| LOL ceiling | headwind | ramp start | desktop Dec-15 | Aug-22 trough |
|--:|--:|---|--:|--:|
| 125,000 | −1,345,000 | 2026-04-01 | 48,520,714 | 43,349,248 |
| 165,000 | −1,345,000 | 2026-04-01 | 48,561,795 | 43,387,545 |
| 165,000 | −1,295,000 | 2026-04-01 | 48,611,795 | 43,415,259 |
| 180,000 | −1,245,000 | 2026-04-01 | 48,672,970 | 43,453,752 |
| 180,000 | −1,245,000 | 2026-07-28 | 48,672,970 | 43,921,488 |
| **200,000** | **−1,245,000** | **2026-07-28** | **48,703,960** | **45,223,249** |

The last row is the ramp re-anchoring: identical Dec-15, +467,737 on the trough, seam discontinuity gone.

**Still not the number to publish.** `o` and `m` remain ~4–5 weeks stale; the headwind anchor has been
attenuated four times running without data-side validation; the LOL ceiling is unfalsifiable against
current data (measurement stopped 2026-06-23). See the caveats cell and `adjustments/_index.md`.

## Current working set

- **Producer / review notebook** — `august_canonical_v2026-07-28.ipynb` (16 cells, executed with
  outputs). The single canonical view: both platform plots, the ex-Iran mobile plot, the Dec-15 table,
  and the caveats. All plots are generated inside the notebook and saved to `plots/`.
- **Adjustment-ladder diagnostic** — `desktop_adjustment_ladder.ipynb` (+ `adjustment_isolation/`).
  Desktop only. Turns each adjustment on one at a time so its individual effect is visible. **Its main
  finding: the seam discontinuity at 2026-07-28 is 100.9% the display-layer headwind**, not the model —
  the raw model output is continuous across the seam, and no parameter value can change that. Also
  measures the `l`/`o` interaction term (−15,590 at Dec-15) and contrasts the two headwind ramp
  conventions. **Read this before starting a parameter search.**
- **Desktop forecast (CANONICAL)** — `desktop_locked/mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet`
- **Desktop forecast (superseded, July's params)** — `desktop_baseline_2026-07-28/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet` — kept: the ledger measures the retune against it, and its raw BQ pull is the shared cache
  (+ sidecar, `parameters.json`). Pre-headwind; `l` (180K) + `o` baked in. **FROZEN — never re-run or rebuilt.** **The directory name is now a
  slight misnomer** — it held the 125K baseline, which this build overwrote in place. Kept as-is because
  the notebook and the committed sidecar reference the path.
- **Mobile forecast** — `mobile_baseline_2026-07-28/cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1/mozaic_daily_forecast.2026-07-28.gm-D.adj-m.parquet`
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
so that needs no model run). The **desktop** command has been run three times into
the same directory, each overwriting the last, as the LOL ceiling was raised: 125K → 165K → 180K → 200K. Runs
after the first reused the cached raw BQ pull already in the slug dir, so no `--raw-cache-dir` was needed
and no BigQuery re-query happened. Logs, in order:
`logs/aug_baseline_{desktop,mobile}_2026-07-28.log` (125K), `logs/aug_lol165_desktop_2026-07-28.log`
(165K), `logs/aug_lol180_desktop_2026-07-28.log` (180K).

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
    --results-dir data-official/2026-08/mobile_baseline_2026-07-28 \
    --changepoint-prior-scale 0.035 --changepoint-range 0.75 --n-changepoints 25 \
    --recent-weeks 13 --holiday-threshold -0.055 --holiday-effect-floor -0.6
```

`run_main.py` **cannot** reproduce these — it has no parameter flags and would use package defaults.
The two param-scan runners are the real producers, and they apply the overlays whose spec's
`applies_to_forecast_start` matches the run date. Logs: `logs/aug_baseline_{desktop,mobile}_2026-07-28.log`.

**The date gate is the trap.** Overlay specs are matched by exact string equality on
`applies_to_forecast_start`. A run at a date no spec claims applies **no** overlays and silently emits
`.raw.` instead of `.adj-lo` / `.adj-m`. The notebook defends against this: `load_all_level_dau` passes
`require_state=["l","o"]` / `["m"]`, so a mis-gated run fails loudly at load instead of producing a
plausible wrong headline.

## Verification built into the notebook

Three checks run as assertions, not eyeballs:

1. **Config lock** — desktop's sidecar is compared field-by-field against the **s01** lock (and its four
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
  desktop_locked/                  # present — CANONICAL (s01, LOL 200K)
  desktop_superseded_lol180k_2026-07-28/  # present — the 180K build it replaced; FROZEN
  desktop_baseline_2026-07-28/     # present — superseded (July's params); ledger baseline + raw cache
  mobile_baseline_2026-07-28/      # present
  adjustments/headwind.json        # present (h)
  launch_on_login/lol.json         # present (l)
  mozillaonline/mozillaonline.json # present (o)
  marketing/marketing.json         # present (m)
  plots/                           # present
  csv/august_canonical_curves.csv  # NOT YET — deliberately deferred (baseline only)
  kpi_sheet/                       # NOT YET
  TODO_factors.md                  # NOT YET — start it as a diff against ../2026-07/TODO_factors.md
```

## Next up

- **Re-measure and swap the remaining two overlay curves** (`o` MozillaOnline, `m` marketing). Both are
  ~4–5 week-stale carry-forwards and each needs a fresh build against data through late July. This is
  now the main reason the forecast is not deliverable. `l` is **rebuilt** (200K, 2026-07-29) though its
  ceiling remains an unfalsifiable judgement.
  Do them one at a time: changing two overlays in one run makes the Dec-15 delta uninterpretable.
- **Validate the Win10 headwind anchor against data.** Now at −1,295,000, attenuated +50,000 from July's
  −1,345,000 (2026-07-29). That is the third successive attenuation on the same rationale
  (−1,420,000 → −1,370,000 → −1,345,000 → −1,295,000), and none of the steps has been checked against a
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
