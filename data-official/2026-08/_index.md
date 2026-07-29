# `data-official/2026-08/` — August 2026 forecast cycle

Active cycle (branch `august-forecast`, off `clean-slate`).

## Status: LOL-165K + headwind −1,295,000 — not yet a delivered forecast

Current build is at forecast_start **2026-07-28** (trained through 2026-07-27) with July's locked
parameters, the **launch-on-login (`l`) ceiling raised 125,000 → 165,000 DAU/day** (rebuilt 2026-07-29),
and the **Win10 headwind desktop anchor attenuated +50,000 to −1,295,000**. `o` and `m` are unchanged
stale carry-forwards from July.

**Dec-15 2026 28d-MA (headwind applied):**

| platform | Aug current | Jul delivered | delta |
|---|--:|--:|--:|
| Desktop | 48,611,795 | 48,585,483 | +26,312 (+0.05%) |
| Mobile | 17,924,607 | 17,923,869 | +738 (+0.00%) |
| **ALL** | **66,536,402** | **66,509,352** | **+27,050 (+0.04%)** |

Aug-22 summer trough (28d-MA, post-headwind): Desktop 43,415,259 · Mobile 17,046,467 · ALL 60,461,726.

### Attribution ledger

Three independent changes separate July's delivered desktop number from the current one. The notebook
computes this ledger and **asserts it closes against the measured value** (residual −0), so a stale
reference constant cannot pass silently:

| desktop Dec-15 28d-MA | step | running |
|---|--:|--:|
| July delivered (125K LOL, hw −1,345,000, 07-06 anchor) | — | 48,585,483 |
| + data refresh to the 07-28 anchor | −64,769 | 48,520,714 |
| + LOL ceiling 125K → 165K | +41,081 | 48,561,795 |
| + Win10 headwind −1,345,000 → −1,295,000 | +50,000 | **48,611,795** |

Two things worth reading off this:

**The LOL cap change passed through nearly one-for-one.** The curve is +40,000/day higher at every
forecast date and the realised Dec-15 effect was +41,081 — a +3% *amplification*, not the material
offset the bidirectional structure might suggest. The extra subtraction lands on only 39 recent training
days (2026-06-19 → 2026-07-27, mean +30,145/day), which barely moves Prophet's fitted trend five months
out, so almost all of the add-back survives. **Do not generalise July's "125K curve netted only +102K"
to marginal cap changes** — that was the absolute effect of introducing the whole curve from zero.

**The headwind step is exactly +50,000 by construction.** `h` is a display-layer adjustment applied to
the 28-day MA, never to the training frame, so its effect is the anchor delta with no Prophet
interaction and it required no model re-run. (At Aug-22 the ramp is only ~55% elapsed, so the trough
moved +27,714, not +50,000.)

**Net: the two upward adjustments together (+91,081) more than offset the data refresh (−64,769)**, so
desktop now sits +26,312 above July's delivered number rather than −64,769 below it.

**Still not the number to publish.** `o` and `m` remain ~4–5 weeks stale, and the headwind anchor has
now been attenuated three cycles running without a data-side validation. See the caveats cell at the
bottom of the notebook and `adjustments/_index.md`.

## Current working set

- **Producer / review notebook** — `august_canonical_v2026-07-28.ipynb` (16 cells, executed with
  outputs). The single canonical view: both platform plots, the ex-Iran mobile plot, the Dec-15 table,
  and the caveats. All plots are generated inside the notebook and saved to `plots/`.
- **Desktop forecast** — `desktop_baseline_2026-07-28/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet`
  (+ sidecar, `parameters.json`). Pre-headwind; `l` (165K) + `o` baked in. **The directory name is now a
  slight misnomer** — it held the 125K baseline, which this build overwrote in place. Kept as-is because
  the notebook and the committed sidecar reference the path.
- **Mobile forecast** — `mobile_baseline_2026-07-28/cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1/mozaic_daily_forecast.2026-07-28.gm-D.adj-m.parquet`
  (+ sidecar, `parameters.json`). Pre-headwind; `m` baked in. **Not rebuilt for the LOL change** — `l`
  is desktop-only, so this is byte-identical to the baseline run and its Dec-15 is unchanged (the
  notebook asserts the drift is exactly 0).
- **Adjustment specs (wired)** — `adjustments/headwind.json` (`h`, display layer),
  `launch_on_login/lol.json` (`l`), `mozillaonline/mozillaonline.json` (`o`),
  `marketing/marketing.json` (`m`). `h`, `o`, and `m` are byte-identical carry-forwards of July's with
  only `applies_to_forecast_start` moved 2026-07-06 → 2026-07-28. **`l` is rebuilt** — see
  `launch_on_login/_index.md`.
- **Iran** — queried natively; the shutdown gap is covered by mozaic's built-in counterfactual fill
  (auto-applied by `populate_tiles`). No cycle-local artifact needed.

## How the current build was produced

The **mobile** command below was run once (2026-07-29 10:0x) and has not been re-run since — `l` is
desktop-only. The **desktop** command was run twice into the same directory: first with July's 125K LOL
curve, then again after the 165K rebuild, overwriting it. The second run reused the cached raw BQ pull
already present in the slug dir, so no `--raw-cache-dir` was needed. Logs:
`logs/aug_baseline_{desktop,mobile}_2026-07-28.log` (125K) and
`logs/aug_lol165_desktop_2026-07-28.log` (165K).


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

1. **Config lock** — both sidecars' `model_config` are compared field-by-field against July's locked
   values (8 params each). Any drift aborts.
2. **State markers** — `load_forecast(..., require_state=...)` pins which adjustments must be present.
3. **Prior-curve reproduction** — July's delivered Dec-15 numbers (48,585,483 / 17,923,869) are
   hardcoded and the rebuilt prior curve must match within 1,000 DAU. Both reproduce at **drift 0**,
   which is what licenses quoting the August-vs-July deltas at all.

## Expected layout (populate as the cycle progresses)

```
2026-08/
  august_canonical_v<date>.ipynb   # producer/review notebook (present)
  desktop_baseline_2026-07-28/     # present
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
  now the main reason the forecast is not deliverable. `l` is **done** (165K, rebuilt 2026-07-29).
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
