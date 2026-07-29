# `data-official/2026-08/` — August 2026 forecast cycle

Active cycle (branch `august-forecast`, off `clean-slate`).

## Status: BASELINE run complete — not a delivered forecast

A **baseline** exists at forecast_start **2026-07-28** (trained through 2026-07-27). It carries July's
locked parameters and all four adjustments (`h`/`l`/`o`/`m`) **unchanged**, so it isolates one variable:
what the July model says on five more weeks of data.

**Dec-15 2026 28d-MA (headwind applied):**

| platform | Aug baseline | Jul delivered | delta |
|---|--:|--:|--:|
| Desktop | 48,520,714 | 48,585,483 | −64,769 (−0.13%) |
| Mobile | 17,924,607 | 17,923,869 | +738 (+0.00%) |
| **ALL** | **66,445,321** | **66,509,352** | **−64,031 (−0.10%)** |

Aug-22 summer trough (28d-MA, post-headwind): Desktop 43,349,248 · Mobile 17,046,467 · ALL 60,395,715.

**This is not the number to publish.** The overlay curves are ~5 weeks stale and the headwind anchor
was deliberately not revisited. See the `[baseline-caveats]` cell of the notebook for the specifics and
the expected direction of each bias.

## Current working set

- **Producer / review notebook** — `august_canonical_v2026-07-28.ipynb` (16 cells, executed with
  outputs). The single canonical view: both platform plots, the ex-Iran mobile plot, the Dec-15 table,
  and the caveats. All plots are generated inside the notebook and saved to `plots/`.
- **Desktop forecast** — `desktop_baseline_2026-07-28/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet`
  (+ sidecar, `parameters.json`). Pre-headwind; `l`+`o` baked in.
- **Mobile forecast** — `mobile_baseline_2026-07-28/cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1/mozaic_daily_forecast.2026-07-28.gm-D.adj-m.parquet`
  (+ sidecar, `parameters.json`). Pre-headwind; `m` baked in.
- **Adjustment specs (wired)** — `adjustments/headwind.json` (`h`, display layer),
  `launch_on_login/lol.json` (`l`), `mozillaonline/mozillaonline.json` (`o`),
  `marketing/marketing.json` (`m`). All four are byte-identical carry-forwards of July's, with only
  `applies_to_forecast_start` moved 2026-07-06 → 2026-07-28.
- **Iran** — queried natively; the shutdown gap is covered by mozaic's built-in counterfactual fill
  (auto-applied by `populate_tiles`). No cycle-local artifact needed.

## How the baseline was produced

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

- **Re-measure and swap the three overlay curves.** All are carried forward stale; each needs a fresh
  build against data through late July. This is the main reason the baseline is not deliverable.
- **Revisit the Win10 headwind anchor.** −1,345,000 is July's value. July softened it from −1,420,000
  on the reasoning that Prophet had partly learned the decline; five more weeks of data plausibly means
  it should attenuate further. Both June and July concluded `adj-h` should shrink as the headwind lands
  in the data.
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
