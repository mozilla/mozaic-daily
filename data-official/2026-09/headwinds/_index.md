# `h` — headwinds, cycle 2026-09

Provenance and rationale for the September Win10 desktop headwind. The spec lives at
`../adjustments/headwind.json` (display layer, live by presence); this directory holds the delivered file, the
value read from it, the plot, and this record.

**What it is:** the Win10 modern-Windows headwind — desktop DAU pressure from the Windows 10 end-of-support
cohort. For September, Brad delivered a **model curve** for the first time (`source_data/win10_headwinds_2026-09-02.csv`,
"August 2026 forecast vintage"): a 28-day-MA linear ramp from 0 at the **August** seam (2026-08-02) to **−726,000** at
2026-12-15, then flat. **It is a draft and a major revision**: August's anchor was −1,315,000.

**How it is applied (decided 2026-09-04).** We take the curve's Dec-15 value and apply it with the shape every
cycle has used: a linear ramp from **0 at this cycle's seam (2026-09-02)** to **−726,000 at 2026-12-15**, then
**flat** (`clamp_at_anchor: true`, matching Brad's file rather than the unclamped tail August published). So the full
headwind lands after the seam, steeper than Brad's ramp.

**What was tried and reverted the same day.** Applying Brad's curve as delivered but shifted to read zero at the
September seam (`--rebase-to-seam`) kept his slope and dropped 166,711 of headwind (−559,289 at Dec-15). The plot
made this visible; the rebase was undone. That path assumed the pre-seam headwind was already in the Prophet base
fit, which was not the intent here.

| | |
|---|---|
| spec | `../adjustments/headwind.json` — `linear_ramp`, desktop −726,000, mobile 0, clamped after Dec-15 |
| delivered file | `source_data/win10_headwinds_2026-09-02.csv` (sha1 in `win10_headwinds_2026-09-02.meta.json`) |
| value read | `dau_28ma` on 2026-12-15 = −726,000 |
| plot | `plots/headwind.linear_ramp.curve.png` |
| Dec-15 desktop effect | −726,000 (exact; display layer, no rerun) |
| change vs August | **+589,000** on the published desktop Dec-15 |
| mobile leg | moved to code `u` (`../tou_mobile_headwind/`), −27,162, a different source |

## What is measured and what is assumed

| quantity | status |
|---|---|
| the −726,000 anchor | **producer's model** (draft) |
| ramp from the seam, flat after Dec-15 | **our convention**, re-anchored each cycle |
| replacing −1,315,000 | **a decision** (2026-09-04) |

## Where new files go

A revised delivery from the producer: drop it in `source_data/`, re-read its Dec-15 value into the spec, update the
meta and this record. If the producer's *shape* is ever to be applied rather than just its anchor, use
`/ingest-adjustment` with the `daily_file` path and do not rebase.
