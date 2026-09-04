# `mobile_cpr0725_2026-07-28/` — LOCKED August mobile build

Locked 2026-08-03. Replaces `mobile_organic_2026-07-28/` (cpr 0.75) as the cycle's mobile build.

| | |
|---|--:|
| Dec-15 2026 28d-MA, pre-headwind | 17,652,224 |
| mobile headwind (`h`) at Dec-15 | −27,162 |
| **Dec-15, post-headwind (the KPI)** | **17,625,062** |
| previous build (cpr 0.75) | 17,601,155 |
| **delta** | **+23,907** |

## Config

Exactly the shipped July lock with **one** change: `prophet_changepoint_range` **0.75 → 0.725**.

```
cps 0.035, cpr 0.725, ncp 25, recent 13, sps 0.1,
regime auto, holiday_threshold -0.055, max_radius 5, min_radius 3, effect_floor -0.6
```

## Why this config

Chosen on **robustness, not size** — every argument for it is independent of Dec-15:

- **It improves the actuals→forecast seam handoff on both measures**: level step −9,989 → **−8,304**,
  slope kink −2,954 → **−1,004 (−66%)**. That is a genuine quality gain unrelated to the target.
- **Not an isolated optimum.** Its measured neighbours are smooth and monotone — cpr 0.775 →
  17,584,021, 0.75 → 17,601,155, 0.725 → 17,625,062. Contrast desktop `g01`, whose seven measured
  one-step neighbours were all 52K–166K shallower.
- **One step inside the probed range**, not an extrapolation.
- **`seasonality_regime` stays `auto`** — mozaic's own volume gate, no override to justify.
- +23,907 is small enough that it does no load-bearing work on the headline.

Full evidence: `research/param-scans/mobile-aug/` (33 probes across three regimes).

## The caveat to carry

cpr 0.725 moves the last allowed changepoint **2025-03-06 → 2025-01-14** — 559 days of recent
history with no changepoint, up from 508. That is the *same direction* as the disqualifying
single-knob extrapolation (cpr ≈ 0.357, which would pin the trend to the 2021–2022 pre-paid-era
regime). Acceptable as one step; **do not push it further** on the strength of this build.

## Provenance

This is the **same artifact** as
`research/param-scans/mobile-aug/results/cps0.035_thresh055_recent13_cpr0.725_ncp25_clip0.6_sps0.1/`
— produced by `scripts/run_mobile_param_scan.py` on the production path (`main(model_configs=...)`,
`organic.json` gating `p`), then promoted here. The `.pkl` is a **hard link** to the scan copy, not
a duplicate; deleting the scan directory will not free it, and will not break this one.

Reproduce with:

```bash
source .venv/bin/activate
python scripts/run_mobile_param_scan.py --forecast-start-date 2026-07-28 \
    --results-dir data-official/2026-08/mobile_cpr0725_2026-07-28 \
    --changepoint-prior-scale 0.035 --changepoint-range 0.725 --n-changepoints 25 \
    --recent-weeks 13 --seasonality-prior-scale 0.1 --seasonality-regime auto \
    --holiday-threshold -0.055 --holiday-effect-floor -0.6
```

## Wiring status — DONE (2026-08-03)

Both steps that were pending when this build was first promoted have since been completed:

- ✅ **The canonical notebook points here.** `august_canonical_v2026-07-28.ipynb` was repointed from
  `mobile_organic_2026-07-28/`, with `EXPECTED_MOBILE_CONFIG.prophet_changepoint_range` 0.75 → 0.725
  and `ORGANIC_MOBILE_DEC15` re-pinned. The pinned value is **17,901,062**, i.e. *including* the
  tailwind below — not this directory's 17,625,062.
- ✅ **The `t` tailwind is adopted and live**, at `../adjustments/tailwind.json`. It is **not** baked
  into this build's parquet — `t` is display-layer, summed into the 28d-MA by the notebook — so the
  numbers in the table above remain this build's own, pre-tailwind.

**So the published August mobile number is not this directory's Dec-15.** Published = 17,625,062
(this build, post-headwind) **+ 276,000** (`t`) = **17,901,062**, of which 1.54% is the overlay.
Rationale and the evidence/judgement split: `../tailwind/_index.md`.

`research/param-scans/mobile-aug/tailwind_exercise.py` remains the sizing script for the +276,000.
