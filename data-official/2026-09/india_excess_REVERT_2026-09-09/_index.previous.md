# `i` — india_excess, cycle 2026-09

**What it is:** India's 2026 desktop DAU running above what a typical year (the 2022–2025 mean of the rebased 28-day curve, by calendar day) would have done, since 2026-05-22, carried forward as real. The delivered daily series is `actual × (2026 − typical) / 2026`, so weekday structure survives subtraction; holidays and India-only dips are bridged out of the ratio only, so the training frame keeps every dip. **Already net of `l`** — India's 5.72% share of the launch-on-login lift (11,450 DAU/day at the edge) is subtracted here; do not net it again. **The cause is a hypothesis**: a university-calendar change is the leading candidate and is not established; report it as "India above typical", not as an education effect. Shipped path **PROPORTIONAL** (a constant 1.58% of India's typical level, so it follows the seasonal calendar), chosen 2026-09-04 as conservative for the time being. Produced by `scratch/brwells/regional-story/india_forecast/deliver.py`; evidence page `site/india_forecast.html`, `DECISIONS.md` D92–D97. `HANDOFF.md` here is the producer's hand-over, kept for the caveats; the pre-ingest `_index.md` is in `../india_excess_REVERT_2026-09-04/_index.previous.md`.

**Family:** per-tile overlay: subtracted from training rows before mozaic and added back after; **needs a model re-run**. **Platform:** desktop (`legacy_desktop`). **Sign:** tailwind (+).

## Files

| file | role |
|---|---|
| `india_excess/india_excess.json` | the spec, gated on `applies_to_forecast_start: 2026-09-02` |
| `india_excess.2026-08-29.parquet` | what the pipeline loads: `india_excess_dau_daily` on a `target_date` DatetimeIndex, `india_excess_dau_ma`, `source` |
| `india_excess.2026-08-29.meta.json` | provenance: source sha1, column mapping, coverage, hold-flat rule, checks |
| `source_data/india_excess.proportional.2026-08-29.csv` | the delivered file, byte for byte |

## Coverage

| | |
|---|---|
| delivered | 2026-01-01 → 2027-12-31 |
| actuals through | 2026-08-29 |
| held flat from | not needed — the delivered file already reaches the horizon |
| horizon | 2026-01-01 → 2027-12-31 |
| Dec-15 28d MA | 41,945 |

## Allocation

Localized: fixed country shares {"IN": 1.0}; excluded: none.

## What is measured and what is assumed

| quantity | status |
|---|---|
| the typical curve (2022–2025) | **measured** |
| the 2026 gap through 2026-08-29 | **measured** — this is what ships as history |
| onset 2026-05-22 | **detected**: first 14-day run above the norm years' maximum after the anchor window; a 7-day sustain and a 1σ rule land on 22 and 24 May |
| holiday and dip bridging | **rule**, symmetric across years, every day listed in the scenario meta |
| India's share of `l` | **computed** from the pipeline's own frame |
| which path persists (PROPORTIONAL) | **an assumption** — a planning choice, conservative for the time being |
| the cause | **a hypothesis** |

## Alternates (kept on disk, decided 2026-09-04)

Five scenario curves were delivered; the spec points at PROPORTIONAL and the other four stay beside it as
`india_excess.{hold,linger,settle,fade}.2026-08-29.{parquet,csv,meta.json}` plus `india_excess.all_scenarios.2026-08-29.csv`.
Net excess, trailing 28-day mean, DAU/day:

| scenario | Dec-15 2026 | Jun-15 2027 | Dec-15 2027 |
|---|--:|--:|--:|
| Hold · the peak excess is carried flat | 57,155 | 57,155 | 57,155 |
| **Proportional · a constant share of the typical level (shipped)** | **41,945** | 41,099 | 41,945 |
| Linger · decays over a year | 33,317 | 20,115 | 12,110 |
| Settle · decays slowly (the handoff's original pointer) | 20,946 | 5,156 | 1,260 |
| Fade · decays fast | 1,161 | 1 | 0 |

Switching scenario is a re-ingest of the corresponding CSV with `--replace`, plus a model rerun. The
handoff's original spec (SETTLE, then PROPORTIONAL) and parquet are in `../india_excess_REVERT_2026-09-04/`.

## Where new files go

A refreshed curve for this cycle: re-run the ingest with `--replace`; the previous build moves to `india_excess_REVERT_<date>/`. Cross-cycle analysis of this effect goes to `research/`.
