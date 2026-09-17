# `data-official/2026-09/csv/` — September 2026 published curves and their scoped twins

| File | What it is | `h` on it? |
|---|---|---|
| `september_canonical_curves.csv` | **The published forecast.** 28-day MA of desktop, mobile and ALL DAU; actuals, August prior, September current | Yes, as the published ramp |
| `september_dec15_summary.csv` | The Dec-15 headline table the notebook prints | Yes |
| `september_desktop_waterfall_steps.csv` | Desktop waterfall steps (OS / market / market group), Aug→Sep and 2025→Sep | Yes, in modern Windows |
| `september_canonical_curves.DESKTOP_ONLY.DAILY.csv` | Desktop only, **DAILY (unsmoothed) DAU**, not a 28-day MA | **Yes**, as the ramp advanced 13.5 days, with the `exact` post-anchor rule (see § The DAILY file) |

⚠️ **Only the first file is the published forecast.** The DAILY file is an unsmoothed view of the
same desktop forecast; it shares column names with the published file but holds a different quantity.
The cycle's headline numbers, attribution ledger and caveats are in `../_index.md`; the full
description of the published file's columns is the same as August's (`../../2026-08/csv/README.md`
§ `august_canonical_curves.csv`) with `july`→`august` and `august`→`september` in the column names.

---

## The `DAILY` file

`september_canonical_curves.DESKTOP_ONLY.DAILY.csv` holds **one raw daily DAU value per row**, not a
28-day MA. It keeps the published file's layout (`date`, `desktop_actuals`, `desktop_prior_august`,
`desktop_current_september`; 2026-01-01 → 2026-12-31; whole DAU) but none of its smoothing: desktop
loses ~40% of its users at weekends, so Dec-15, a Tuesday, reads **55,802,968** where the published
MA reads 49,332,443. Built 2026-09-17 by `scripts/export_desktop_daily_csv.py` from the same two
desktop parquets as the published file (`../desktop_g01_2026-09-09/…adj-ijlo.parquet` and
`../../2026-08/desktop_g01_2026-08-02/…adj-lo.parquet`). The August column is the August daily
export (`../../2026-08/csv/august_canonical_curves.DESKTOP_ONLY.DAILY.csv`) reproduced: 55,077,204 at Dec-15.

### ⚠️ This file is not the published forecast

The columns share names with the published file but hold a different quantity. Only the filename
says "daily", so a loader pointed at the wrong file reads numbers ±20% off and raises nothing. The
published headline is the 28-day MA in `september_canonical_curves.csv`. This file re-smooths to it
(see below) but is not it.

### How the file carries `h`

The pipeline applies the Win10 headwind `h` to the 28-day MA and nowhere else; no daily row in this
repo carries it natively. A trailing 28-day mean of a linear ramp **lags the ramp by 13.5 days**, so
the consistent daily headwind is the published ramp **advanced by 13.5 days**:

```
daily_h(t) = ramp(t + 13.5 days)   for t ≥ seam,   0 on training rows
```

With it, `rolling(28).mean()` of the file reproduces the published column **to the DAU from
seam + 27 onward**, asserted by the script on write. Full derivation: August's README § The `DAILY` file.

### New in September: the ramp is clamped, so the daily headwind sawtooths after Dec-15

September's `h` spec carries `clamp_at_anchor: true`: the published ramp runs from 0 at the
2026-09-09 seam to −1,017,277 at Dec-15 and is **flat after that**. The 13.5-day advance is exact only
while the whole 28-day window sits on one straight piece, so around the Dec-15 kink **no smooth daily
headwind re-smooths to the published curve**. Three rules were computed (`--post-anchor-rule`) and
Brendan chose **`exact`** on 2026-09-17:

| rule | re-smooths exactly | daily headwind after Dec-15 | Dec-15 daily |
|---|---|---|---|
| **`exact` (shipped)** | 2026-10-06 → **2026-12-31**, 0 DAU gap | jumps from −1,158,857 (Dec-15) to −875,697 (Dec-16), then ramps down to −1,033,008 by Dec-31: a 28-day sawtooth, the only series whose flat 28-day mean is the anchor | **55,802,968** |
| `flat_at_anchor` | 2026-10-06 → 2026-12-15 | held at −1,017,277; file re-smooths up to 36,706 too deep by Dec-31 | 55,802,968 |
| `advanced_clamped` | 2026-10-06 → 2026-12-01 and from 12-29 | flat at −1,017,277 from Dec-2; 36,706 too shallow at Dec-15 itself | 55,944,548 |

The sawtooth is forced arithmetic, not a judgement: a 28-day mean that stops moving while its
history is a ramp can only stay flat if each new day repeats the day 28 earlier. **Do not read the
post-Dec-15 daily headwind as a forecast of anything.** Quote the anchor (−1,017,277) from the
published file; the daily values after Dec-15 exist so the file re-smooths to it.

### The weirdness ledger (printed by the script)

| | September (`desktop_current_september`) | August (`desktop_prior_august`) |
|---|---|---|
| spec | 0 at 2026-09-09 → −1,017,277 at Dec-15, **then flat** | 0 at 2026-08-02 → −1,315,000 at Dec-15, unclamped |
| slope | −10,487.4 DAU/day | −9,740.7 DAU/day |
| 13.5-day advance | −141,580 | −131,500 |
| daily headwind on the seam day | **−141,580** | **−131,500** |
| daily headwind at Dec-15 | **−1,158,857** (anchor −1,017,277) | **−1,446,500** (anchor −1,315,000) |
| daily headwind Dec-16 / Dec-31 | −875,697 / −1,033,008 (sawtooth) | keeps ramping |
| first forecast week vs last actual week | +399,620 DAU/day, of which −173,042 is headwind | −1,106,722, of which −160,722 |
| file re-smooths to the published curve from | 2026-10-06 | 2026-08-29 |
| max discrepancy inside the transition | 65,319 DAU | 101,373 DAU |

1. **The headwind starts with a step** of 13.5 slopes on the seam day. The published MA carries the
   ramp's full daily increment from its first forecast day, when 27 of its 28 window days are actuals;
   no daily series that is zero on actuals can match that without the step.
2. **At Dec-15 the daily headwind is 141,580 deeper than the anchor.** Quote the anchor from the
   published file.
3. **For 27 days after each seam the file does not re-smooth to the published curve**: there the
   published curve is `display_ma`'s variance-matched splice, non-linear in an added ramp.
4. **Actuals end 2026-09-08**, one day before the published file's, because the parquet's training
   rows stop at training-end. Their 28-day mean matches the published actuals to within 1.036 DAU: one
   August day landed a single DAU different between the parquet's pull and the re-pulled published
   actuals (1/28 of a DAU on the mean) plus whole-DAU rounding. The August column's pre-seam rows are
   August's own training rows through 2026-08-01; they differ from September's pull by at most 4 DAU.
5. **Only `h` rides on the file.** `i`, `j`, `l`, `o` are baked into the parquet and cannot come
   off; `t` and `u` are mobile-only. The script refuses non-`linear_ramp` specs.

Verification plot: `../plots/desktop_daily_export_vs_published_ma.png` (daily file over the published
MA; bottom panel is re-smoothed minus published, zero outside the two transition windows).

### Provenance

```bash
python scripts/export_desktop_daily_csv.py                       # write + plot + verify (rule 'exact')
python scripts/export_desktop_daily_csv.py --dry-run --post-anchor-rule flat_at_anchor   # ledger only
```

Tests: `tests/test_export_desktop_daily_csv.py` (lock the advance, the three clamp rules and the
verify window; the real-build test reproduces August's 55,077,204 when the parquets are on disk).
