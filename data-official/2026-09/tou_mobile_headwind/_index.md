# `u` — tou_mobile_headwind, cycle 2026-09

Rationale record for the mobile terms-of-use headwind. The spec lives at `../adjustments/tou_mobile_headwind.json`
(display layer, live by presence); this directory holds only prose, like `../../2026-08/tailwind/` did for `t`.

**What it is:** a mobile DAU headwind carried against the risk attached to the terms-of-use acceptance requirement
applied in 2025. Linear ramp from 0 at the seam (2026-09-02) to **−27,162** at 2026-12-15, mobile only.

**Where it came from.** Through August 2026 this number was the `mobile_dau` leg of `adjustments/headwind.json`,
sharing a file with the Win10 desktop headwind. On 2026-09-04 the desktop headwind was replaced by Brad's model
curve (`../headwinds/`), and the mobile leg — a different source, sized on different evidence — was given its own
code so the two can move independently and neither can hide inside the other's line.

**The number is unchanged from August** (−27,162 at Dec-15, the same anchor since June 2026). Nothing about the
terms-of-use risk was re-measured this cycle; this is a carry-forward, and the ledger records it as a zero change.

| | |
|---|---|
| spec | `../adjustments/tou_mobile_headwind.json` |
| registration | `../../adjustment_codes.yaml`, code `u` |
| Dec-15 mobile effect | −27,162 (exact; display layer) |
| sign | headwind (−) |

## Where new files go

A re-sized anchor is an edit to the spec's `mobile_dau` plus a line here recording the old value and the reason.
A modelled curve would go through `/ingest-adjustment` as a `daily_file` and would put its parquet, source and
plot in this directory.
