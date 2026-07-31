# Desktop — August 2026 CANONICAL (forecast_start 2026-07-28, **g01** config, LOL **200K**)

`legacy_desktop` DAU. **This is the canonical August desktop build.** Adopted 2026-07-30, replacing
the **s01** config that held this slot from 2026-07-29. Not yet a *delivered* forecast — `o` and `m`
remain stale carry-forwards; see `../_index.md`.

## The predecessor is preserved — a revert is expected to be possible

**`../desktop_s01_REVERT_2026-07-29/` is a REVERT TARGET, not an archive.** It holds the complete
s01 build, the README it carried while canonical, and the headwind spec that went with it.
`REVERT.md` there gives the exact steps. **Do not delete it while August is the live cycle.**

**Two things changed together and revert as one unit:**

| # | change | from | to |
|---|---|---|---|
| 1 | model config | s01 (`cps 0.1849, cpr 0.734, ncp 35, recent 17`) | **g01** (`cps 0.1649, cpr 0.814, ncp 40, recent 17`) |
| 2 | Win10 headwind desktop anchor | −1,245,000 | **−1,220,000** |

The headwind moved because the config change alone dropped Dec-15 by 31,357; +25,000 of attenuation
absorbs most of it. Reverting the config without the headwind would leave Dec-15 25,000 above where
s01 published.

## Config

| param | value |
|---|--:|
| `prophet_changepoint_prior_scale` | 0.1649 |
| `prophet_changepoint_range` | 0.814 |
| `prophet_n_changepoints` | 40 |
| `prophet_recent_weeks` | 17 |
| `prophet_seasonality_prior_scale` | 0.00825 |
| `seasonality_regime` | multiplicative |
| holiday knobs (all four) | package defaults — excluded from tuning by policy |

The canonical notebook asserts all six against this lock plus the four holiday defaults, and aborts
on drift.

## Result (28d-MA, post-headwind −1,220,000 ramping from the seam)

| quantity | g01 (this build) | s01 (predecessor, at −1,245,000) | delta |
|---|--:|--:|--:|
| **Aug-25 trough minimum** | **45,041,389** | 45,223,249 | **−181,860** |
| Aug-22 | 45,091,364 | 45,263,042 | −171,678 |
| **Dec-15** | **48,697,603** | 48,703,960 | **−6,357** |
| seam kink (model-only) | −16,549 | −9,554 | −6,996 (**1.73×**) |
| trough date | 2026-08-25 | 2026-08-25 | unmoved |

Net vs July delivered: **+112,120 (+0.23%)**. ALL (desktop+mobile) Dec-15: **66,622,210**.

Config-isolated, both scored at −1,245,000: Aug-25 **−186,860**, Dec-15 **−31,357**.

**The +25,000 headwind buys back 25,000 at Dec-15 but gives up only 5,000 at Aug-25** — the ramp is
20% accrued at the trough (28 of 140 days), so the trade is deliberately asymmetric in our favour.

## Why g01: closing part of the Aug-25 gap to July

August sat **1,961,825** above July's delivered curve at the trough. The ask was to close 10% of it
(−196,183). g01 closes **186,860 config-isolated = 9.52%** of the gap, or 9.27% net of the headwind
give-back.

## Read this before relying on the trough number

**g01 is an ISOLATED optimum.** It was the single deepest cell of a 243-cell full factorial, and
**all seven of its measured one-step neighbours are 52,092–165,860 shallower** at Aug-25. It is
deterministic and reproduces exactly — the parquet was regenerated from scratch on 2026-07-30 and
matched to the DAU — so this is **not** a reproducibility risk. The risk is fragility: any future
data refresh, package upgrade, or re-tune that shifts an effective parameter slightly will likely
lose most of the 9.52% while keeping the 1.73× kink cost.

**A ~1.44× kink regression was unavoidable.** Across 370 scored configurations, every one of the 43
satisfying both hard constraints carried a kink penalty of +4,176 to +6,996 over s01. No config
moves Aug-25 meaningfully and keeps the seam slope near s01's. g01 sits near the top of that range
because it buys the most movement — an accepted trade, not an oversight.

**Why five one-at-a-time search rounds missed this config:** `ncp` carries 40.6% of Aug-25 variance
but reads as *inert* at `cpr` 0.734. Its potency is conditional on `cpr` (the `cpr:ncp` interaction
is 18.5% of Aug-25 variance), and one-at-a-time probing is structurally blind to that.

Full evidence, all seven rounds, and figures: `../../../research/param-scans/aug25-gap/`
(`LOG.md`, `plots/candidates_vs_locked.png`, `plots/round6_grid.png`).

## Reproducing

```bash
source .venv/bin/activate
python scripts/run_param_scan.py --forecast-start-date 2026-07-28 \
    --results-dir research/param-scans/aug25-gap/runs \
    --raw-cache-dir data-official/2026-08/desktop_baseline_2026-07-28/cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825 \
    --seasonality-regime multiplicative \
    --changepoint-prior-scale 0.1649 --changepoint-range 0.814 \
    --n-changepoints 40 --recent-weeks 17 --seasonality-prior-scale 0.00825 \
    --holiday-threshold -0.032 --holiday-max-radius 5 \
    --holiday-min-radius 3 --holiday-effect-floor -0.6
```

Score with `python scripts/score_near_horizon.py <parquet> --target-date 2026-08-25`. **Ignore that
tool's `target band : 45M-46M` line** — it belongs to the earlier raise-the-trough objective, the
opposite of what g01 was adopted for.

The raw BigQuery pull is shared and unchanged, so neither this build nor a revert needs a re-query.

## Contents

| file | what |
|---|---|
| `mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet` (+ `.meta.json`) | the forecast, `l`(200K)+`o` baked in, pre-headwind |
| `parameters.json` | the g01 config |
| `mozaic_objects.legacy_desktop.2026-07-28.pkl` | fitted mozaic objects (634MB, gitignored) |
| `mozaic_parts.raw.legacy.desktop.DAU.parquet` | symlink to the shared raw BQ pull |
