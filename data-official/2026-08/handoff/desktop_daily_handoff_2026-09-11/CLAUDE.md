# For an AI assistant reading this folder

You are looking at a **desktop-only, daily-resolution (unsmoothed)** export of Mozilla's August 2026
Firefox desktop DAU forecast. Read this before answering any question about the numbers.

## Files

- `august_canonical_curves.DESKTOP_ONLY.DAILY.csv` — the deliverable. Columns `date`,
  `desktop_actuals`, `desktop_prior_july`, `desktop_current_august`. 365 rows, 2026-01-01..2026-12-31,
  whole DAU, one **raw daily** value per row. Weekly seasonality intact (weekends ≈ −40%).
- `reference/august_canonical_curves.PUBLISHED_28D_MA.csv` — the published forecast. **Same column
  names, different quantity: 28-day trailing moving averages.** Also carries `mobile_*` and `all_*`.
- `reference/headwind.{august,july}.json` — `linear_ramp` specs for the Win10 headwind each forecast
  column carries.
- `plots/desktop_daily_export_vs_published_ma.png` — daily vs published MA, plus the verification panel.
- `README.md` — the human explanation; same facts, prose form.

## Column semantics (do not infer these from the data)

| column | rows | content |
|---|---|---|
| `desktop_actuals` | 2026-01-01..2026-08-01 | measured DAU; NaN after |
| `desktop_prior_july` | all | actuals through 2026-07-05, then the **July** forecast (seam 2026-07-06) with July's headwind |
| `desktop_current_august` | 2026-08-02.. | the **August** forecast (seam 2026-08-02) with August's headwind; NaN before |

## Invariants you can rely on (all asserted at export time)

1. `rolling(28).mean()` of a forecast column — with `desktop_actuals` filling the NaN pre-seam rows —
   equals the published column **to within 1 DAU from seam+27 onward**: from 2026-08-29 for
   `desktop_current_august`, from 2026-08-02 for `desktop_prior_july`, through 2026-12-31.
2. `rolling(28).mean()` of `desktop_actuals` equals the published `desktop_actuals` (≤1 DAU) wherever both exist.
3. Inside the 27 days after each seam invariant 1 does **not** hold (published values there are a
   variance-matched splice, non-linear in the daily series). Max discrepancy: 101,373 (August), 469,412 (July).
4. No mobile or combined columns exist in the daily file. Do not sum anything to get "ALL".

## The headwind: the one non-obvious construction

- The Win10 headwind `h` is a display-layer adjustment defined **on the 28-day MA**: a linear ramp,
  0 at the seam → `desktop_dau` at `anchor_date` (August: −1,315,000 at 2026-12-15; July's spec starts
  2026-04-01, so at July's seam it is already −500,465).
- A trailing 28-day mean of a linear ramp lags it by 13.5 days. To satisfy invariant 1 the daily
  headwind in this file is the MA-space ramp **advanced 13.5 days**: `daily_h(t) = 0.5·(ramp(t+13) + ramp(t+14))`
  for `t ≥ seam`, 0 on training rows. Exact for a linear ramp.
- Consequences to state, not hide, if asked:
  - seam-day daily headwind: **−131,500** (August), **−570,843** (July); not zero.
  - Dec-15 daily headwind: **−1,446,500** (August), **−1,415,378** (July); deeper than the anchors
    (−1,315,000 / −1,345,000) by 13.5 slopes. **The published anchor is the number to quote.**
  - Adding the published ramp per day and re-smoothing would land exactly +131,500 above the published
    curve; that is the wrong construction and was tested.
- Slopes: August −9,740.7 DAU/day (135 days); July −5,213.2 DAU/day (258 days).

## Answering common questions

- "What is the Dec-15 forecast?" → The published headline is the **28-day MA, 48,703,443**. The daily
  file's Dec-15 value (55,077,204, a Tuesday) is a single day and is not the headline.
- "Why is Aug 2 lower than Aug 1?" → Weekday structure (Sat → Sun), not the seam. Compare like weekdays
  or week means. The headwind's seam-day step (−131,500) is ~0.3% of the level.
- "Can I remove the headwind?" → Yes, exactly: subtract `daily_h(t)` as defined above from the forecast
  rows. Do not subtract the MA-space ramp from daily rows.
- "Can I get mobile / ALL daily?" → Not from this folder; ask the author.
- "Is this the current forecast?" → It is the **August 2026** cycle as of 2026-08-04. A September cycle exists separately.

## What is baked into the forecast rows (not removable here)

The August desktop model (Prophet per-country tiles, reconciled top-down), the Launch at Login
new-users tailwind (`l`), and the MozillaOnline China-migration tailwind (`o`). Only `h` is a
display-layer addition. Model and pipeline: `mozaic-daily` repo, script `scripts/export_desktop_daily_csv.py`.
