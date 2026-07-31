# `desktop_candidate_aug25/` — CANDIDATE, not canonical

**Status: CANDIDATE. `../desktop_locked/` remains the canonical August desktop build and has not
been touched.** Promotion requires an explicit second approval — see
`../../../research/param-scans/aug25-gap/PLAN.md` § Promotion.

## Read this first — the cost

**This config regresses the seam kink by 1.50×** (−14,285 vs s01's −9,554), and it delivers **66%**
of the requested Aug-25 move, not 100%.

The regression is **not avoidable by tuning**. Across 370 scored configurations, every one of the 43
that satisfies both hard constraints carries a kink penalty between **+4,176 and +6,996**. There is
no feasible config that moves Aug-25 meaningfully and keeps the kink near s01. A ~1.44× floor is the
price of admission; this candidate sits at 1.50×, near that floor.

Whether that trade is worth making is a judgement call, and it should be made explicitly rather than
inherited from this directory's existence.

## Config

Differs from the locked **s01** config in four knobs:

| param | s01 (canonical) | this candidate |
|---|--:|--:|
| `prophet_changepoint_prior_scale` | 0.1849 | **0.2049** |
| `prophet_changepoint_range` | 0.734 | **0.814** |
| `prophet_n_changepoints` | 35 | **40** |
| `prophet_recent_weeks` | 17 | **14** |
| `prophet_seasonality_prior_scale` | 0.00825 | 0.00825 |
| `seasonality_regime` | multiplicative | multiplicative |
| all four holiday knobs | package defaults | package defaults |

Overlays `l` (LOL 200K) and `o` baked in, same as canonical. Pre-headwind. Same forecast_start
(2026-07-28) and the same shared raw BigQuery pull — **no re-query**, so every difference from
canonical is attributable to these four parameters.

## Scored result (desktop ALL, 28d MA via `display_ma`, post-headwind)

| metric | canonical (s01) | this candidate | delta | budget used |
|---|--:|--:|--:|--:|
| **Aug-25** | 45,223,249 | **45,094,241** | **−129,008** | 90% of the ±75,000 band |
| **Dec-15** | 48,703,960 | **48,704,340** | **+380** | **1% of the ±50,000 cap** |
| seam kink (model-only) | −9,554 | −14,285 | −4,731 | 1.50× |
| trough date | 2026-08-25 | 2026-08-25 | unmoved | — |

- Aug-25 lands **+67,175** above the 45,027,066 target — inside the ±75,000 hard band.
- **Dec-15 drift is +380, effectively zero.** No headwind compensation is needed, and the full
  headwind lever stays in reserve.
- Verified by re-scoring the parquet directly, and reproduced bit-for-bit on an independent re-run
  (which also confirms Prophet is deterministic for a fixed config here).

## Why this point and not another

The selection rule (set 2026-07-30): Aug-25 within ±75,000 **hard**, Dec-15 within ±50,000 **hard**
preferring ≤40,000, then **minimise the kink increase**.

Strictly minimising kink selects a different cell (kink +4,274) — but that one sits at **99%** of the
Aug-25 band. Giving up 457 of kink penalty buys margin on *both* hard constraints and takes Dec-15
from +25,475 to +380. The strict optimum was rejected as too close to a boundary; this was a
deliberate, approved choice.

The full Pareto frontier of Aug-25 accuracy against kink is in
`../../../research/param-scans/aug25-gap/LOG.md` § Round 7.

## What it took to find

Seven rounds, 370 scored configurations. The one-at-a-time rounds 1–5 concluded the target was
unreachable and topped out at 14% of the move; they were wrong because **`ncp` carries 40.6% of
Aug-25 variance but reads as inert at `cpr` 0.734**. Its potency is conditional on `cpr`, and a
one-at-a-time probe is structurally blind to that. The 243-cell factorial in Round 6 measured the
`cpr:ncp` interaction at 18.5% of Aug-25 variance and found the region this candidate sits in.

## Reproducing

```bash
source .venv/bin/activate
python scripts/run_param_scan.py --forecast-start-date 2026-07-28 \
    --results-dir research/param-scans/aug25-gap/runs \
    --raw-cache-dir data-official/2026-08/desktop_baseline_2026-07-28/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825 \
    --seasonality-regime multiplicative \
    --changepoint-prior-scale 0.2049 --changepoint-range 0.814 \
    --n-changepoints 40 --recent-weeks 14 --seasonality-prior-scale 0.00825 \
    --holiday-threshold -0.032 --holiday-max-radius 5 \
    --holiday-min-radius 3 --holiday-effect-floor -0.6
```

Score with `python scripts/score_near_horizon.py <parquet> --target-date 2026-08-25`. **Ignore that
tool's `target band : 45M-46M` line** — it belongs to an earlier, opposite objective (raising the
trough) and is unrelated to this candidate's criteria.

## If this is promoted

Not done here; each needs its own decision:

1. `../_index.md` status section and the attribution ledger need a row for the config change.
2. `../august_canonical_v2026-07-28.ipynb` asserts the desktop sidecar field-by-field against **s01**
   and will abort on this config. Update that lock deliberately — do not bypass it.
3. The seam-kink regression should be reviewed against this cycle's seam-artifact history
   (`research/ma-seam-turbulence/LOG.md`) before publishing near-horizon numbers.
4. Mobile is unaffected — these are desktop-only knobs and mobile was not re-run.
