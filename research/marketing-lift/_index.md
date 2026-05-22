# `research/marketing-lift/` — Fenix Android paid-marketing DAU lift

Mobile DAU lift attributable to the Fenix Android paid campaign that launched 2026-04-06. The production adjustment is registered as code `m` in `data-official/adjustment_codes.yaml` and consumed via the per-tile bidirectional applier in `src/mozaic_daily/adjustments.py`.

## Approach lineage

| Subdir | Status | Approach |
|---|---|---|
| `v1-convolution/` | **superseded** — kept for reference | Modeled lift as a convolution of paid-acquisition impulses with a retention kernel; produced larger end-of-year lift than supported by actuals |
| `v2-real-data/` | **current** | Marketing-team CSV provides Paid-DAU directly; hybrid stitches empirical Fenix gap historical with additive CSV future |

The hybrid in v2 lands about 30% the size of the v1 convolution forecast at Dec-15 — closer to the leadership-validated number.

## Top-level notebooks

| File | What it does |
|---|---|
| `marketing_lift_validation.ipynb` | End-to-end validator: with-vs-without marketing comparison, MA28 diagnostics, Dec-15 readout |
| `mobile_marketing_paid_organic.ipynb` | Decomposes mobile DAU into paid vs. organic contribution from the campaign |

## Producers and consumers

- Adjustment spec lives in `data-official/{YYYY-MM}/marketing/marketing.json` (consumed by the `m` adjustment applier)
- Daily lift parquet lives next to that JSON; produced by the notebooks here
- Pipeline integration: `python scripts/run_main.py --no-marketing-lift` disables; default applies `m` when the spec matches the forecast start
