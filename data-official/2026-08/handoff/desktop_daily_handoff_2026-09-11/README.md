# August 2026 desktop DAU forecast, daily and unsmoothed

Brendan Wells (brwells@mozilla.com) prepared this folder on 2026-09-11. Send questions to me.

## What is in this folder

| File | What it is |
|---|---|
| `august_canonical_curves.DESKTOP_ONLY.DAILY.csv` | **The deliverable.** Desktop DAU, one raw daily value per row, 2026-01-01 to 2026-12-31. |
| `plots/desktop_daily_export_vs_published_ma.png` | The daily file plotted over the published 28-day moving average, with a verification panel. |
| `reference/august_canonical_curves.PUBLISHED_28D_MA.csv` | The published August forecast (28-day moving averages for desktop, mobile and combined), for cross-checking. |
| `reference/headwind.august.json`, `reference/headwind.july.json` | The Win10 headwind specs behind each forecast column. |
| `CLAUDE.md` | The same explanation, written for an AI assistant. Point Claude or any model at this folder and it will know how to read the file. |

## The CSV

The columns match the published file in name and layout:

| Column | Meaning |
|---|---|
| `date` | Calendar day. |
| `desktop_actuals` | Measured desktop DAU from 2026-01-01 to 2026-08-01, blank after. |
| `desktop_prior_july` | Actuals through 2026-07-05, then the **July 2026** forecast (seam 2026-07-06). For comparison. |
| `desktop_current_august` | Blank before 2026-08-02, then the **August 2026** forecast (seam 2026-08-02). The current forecast. |

Each value counts whole daily active users, and **nothing is smoothed**. The weekly cycle is
intact, and desktop loses about 40% of its users at weekends. That is why this file reads
**55,077,204** for Dec-15, a Tuesday, while the published 28-day average for the same day
reads **48,703,443**. The two numbers describe the same forecast: one is a day, the other a
28-day mean.

## How this file relates to the published forecast

The published forecast is a trailing 28-day average. This file is the daily series underneath
it. Take a trailing 28-day mean of either forecast column and you recover the published column
to the user, from 27 days after each seam through Dec-31 (from 2026-08-29 for the August
column, from 2026-08-02 for the July column). The bottom panel of the plot shows that
difference sitting at zero. The export script asserted it; nobody eyeballed it.

For the 27 days after each seam the two disagree, by up to about 100K for August and 470K for
July. There the published curve is not a plain rolling mean but a variance-matched transition
that blends actuals into forecast so the average does not wobble. The published file already
flags those dates as a transition.

## What you must know about the Win10 headwind

The published forecast carries a Windows 10 headwind: a straight line falling from 0 at the
seam to **−1,315,000** DAU on 2026-12-15. The pipeline defines and applies that line on the
**28-day average**, never on daily data, so a daily file that carries the headwind had to be
derived.

A trailing 28-day mean of a straight line lags the line by 13.5 days. To make this file's
rolling mean match the published curve, the daily headwind is therefore the published ramp
**shifted 13.5 days earlier**. You will see three consequences:

1. **The headwind does not start at zero.** On the first forecast day, 2026-08-02, the daily
   series already carries −131,500: 13.5 days of the ramp's 9,741 DAU per day. The July column
   carries −570,843 on its seam day, because July's spec had been ramping since 2026-04-01.
2. **On Dec-15 the daily headwind is −1,446,500**, deeper than the published anchor of
   −1,315,000 by the same 131,500. If you quote the headwind, quote the published anchor. The
   daily value is that anchor expressed in daily space, not a different number.
3. **Adding the published ramp to the daily series yourself will overshoot.** Re-smooth that and
   you land exactly 131,500 above the published curve on every date. This construction avoids
   that mistake.

## A check you can run

```python
import pandas as pd
d = pd.read_csv("august_canonical_curves.DESKTOP_ONLY.DAILY.csv", parse_dates=["date"]).set_index("date")
m = pd.read_csv("reference/august_canonical_curves.PUBLISHED_28D_MA.csv", parse_dates=["date"]).set_index("date")
col = "desktop_current_august"
filled = d[col].where(d[col].notna(), d["desktop_actuals"])   # actuals fill the pre-seam window
diff = filled.rolling(28).mean() - m[col]
print(diff["2026-08-29":].abs().max())   # -> <= 1.0 (rounding)
print(d.loc["2026-12-15", col], m.loc["2026-12-15", col])   # 55077204 vs 48703443
```

## What the numbers include, and what they leave out

- **Included:** the August model (Prophet fits per country, reconciled top-down), the Launch at
  Login tailwind for new users, the MozillaOnline migration tailwind for China, and the Win10
  headwind described above.
- **Left out:** mobile (this file is desktop only), the mobile tailwind, and every decision made
  after 2026-08-04. The September forecast is a separate cycle.
- Actuals stop on 2026-08-01, one day before the published file's, because they come from the
  model's training rows.
