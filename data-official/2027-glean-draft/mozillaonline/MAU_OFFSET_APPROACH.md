# China MozillaOnline migration: the MAU offset and the population split (approach of record)

**Status:** approach written 2026-10-12 before building; the producer (`scripts/build_glean_history_cn_offsets.py`)
implements exactly this and raises if its checks fail. Decision: Brendan, 2026-10-12 ("write your approach and run
the checks, then rerun the forecast").

## Problem

The delivered MozillaOnline curve is a **DAU, Windows-only** daily series (official export 2026-09-02, held flat
from 2027-01-01 at 644,169/day). Applied through the `o` mechanism it repaired China × windows DAU (build 3), but
in the Glean-equivalent OS × country data the June 2026 step is also visible on **China × mac** (the cell doubled,
31K → 63K DAU) and on **MAU**, the headline metric, which the DAU-only overlay machinery never touched. Both China
MAU tiles and the China mac DAU tile forecast nonsense (negative values, 9M → 2M swings).

## What the history says (export fc15 cells, Glean-equivalent, pre = 2026-04-15..05-31 DAU / 05-15..05-31 MAU, post = 2026-08-01..09-08 DAU / 08-15..09-08 MAU)

| China cell | DAU step | MAU step | MAU per DAU of the step | baseline MAU per DAU |
|---|--:|--:|--:|--:|
| windows | +770,000 | +2,220,000 | 2.88 | 3.74 |
| mac | +27,600 | +88,500 | 3.20 | 3.41 |
| linux | ~0 | −94,000 | not a step | 7.72 |

The migrated population is more engaged than China's baseline (fewer monthly users per daily user), so scaling by
the baseline ratio would overshoot by about 30%. The step's own ratio is the multiplier. The delivered DAU curve's
August mean (748K) matches the export's Windows step (723K) once the country shares are renormalized over our 16
countries (CN gets 0.962 of the world curve), so the DAU side is consistent with the data it is correcting.

## Construction

1. **MAU world curve** = trailing 28-day mean of the DAU world curve × **r**, where
   r = (ΣMAU step) / (ΣDAU step) over windows + mac = 2.894. A trailing 28-day mean is the MAU analogue of a daily
   DAU series (the delivered file already carries `dau_28ma`); the step ratio turns migrated daily users into
   migrated monthly users. Held flat where the DAU curve is held flat (2027).
2. **Population shares** (the split across `windows` / `mac` / `linux`), per metric, from the measured steps:
   DAU 96.5% / 3.5% / 0; MAU 96.2% / 3.8% / 0. Negative steps (linux) are clipped to zero.
3. **Country shares, scope, gate** unchanged from the DAU spec (CN 0.9277 of the world curve, HK/US/JP/SG/TW/DE/AU/ROW
   the rest, IR excluded, renormalized over training-present countries; gated to the seam).
4. **Spec**: the project-local `mozillaonline.glean_history.json` gains a `metrics` block (DAU and MAU curves) and
   `allocation.population_shares` per metric. `overlays.py` reads both; specs without them behave exactly as before
   (production is unaffected; test-locked).

## Checks (the producer raises on any failure)

- **No step after subtraction.** For each (metric, population) with a share > 0, China's training series after the
  overlay subtraction, applied through the real `subtract_lift_from_training` with the spec's shares, must have
  |mean(2026-07-15..09-08) / mean(pre window) − 1| < 15% (raw: +72% windows DAU, +56% windows MAU).
- **Round trip.** Subtract then add back (the real `add_lift_to_forecast`) returns China's training rows to the raw
  export within rounding (|Δ| ≤ 1 per row).
- **Sanity.** Ratios in [1, 8]; MAU curve non-negative, dense daily, zero before 2026-06-01; population shares sum
  to 1 per metric.
- **Plot** `plots/cn_offsets_check.png`: China windows and mac, DAU and MAU, raw vs after subtraction, 2025-07 → 2026-10,
  with the pre/post windows shaded. The check a reader verifies by eye.

## What is assumed, and would move the number

- The MAU/DAU ratio of the migrated users (2.88 / 3.20) is measured over three months of a still-evolving migration,
  on pooled-factor cells for Mac. Nobody has measured MozillaOnline MAU directly: **this is a derived curve.**
- If the migrated users' engagement drifts in 2027, the offset will not follow (it is a fixed multiple of the DAU curve).
- The Linux step is treated as zero; the −94K Linux MAU step is left to the model.
- Re-measure the ratios if the export agent re-issues the history (Fx 155 reference change).

## Outputs

`mozillaonline/mozillaonline_migration_mau.<date>.parquet` (+ `.meta.json` with the measured steps, ratios, shares,
source sha1s), the updated spec, `plots/cn_offsets_check.png`, and `cn_offsets_measurements.json`.
