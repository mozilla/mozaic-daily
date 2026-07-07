# desktop-gap-decomp

Decomposing the gap between actual desktop DAU and the forecast by coarse region,
to explain why actuals are riding above what the model expected (the Looker
"Desktop" dashboard, 28d-MA).

## What's here
- `make_pancake.py` — two-panel figure. Top: the actual-vs-forecast disconnect,
  zoomed (2026 actuals, the canonical **June** forecast 2026-05-26 → year-end,
  the April prior forecast, and 2025 calendar-aligned actuals). Bottom: the
  stacked "pancake" of `legacy_desktop` DAU (28d-MA) by region (CN / US / EU / ROW)
  with the June forecast total overlaid, plus a Jan-1→now rise attribution.
  Forecast source is `data-official/2026-06/csv/june_canonical_curves.csv`
  (`desktop_current_june_plus_iran`); actuals are fresh mart pulls that match that
  CSV's actuals to the dollar over their overlap.
- `pull.sql` — the BigQuery pull (mart_mozaic_daily_forecast_v2, legacy_desktop,
  segment os=ALL, per-country + ALL).
- `region_daily.csv` — daily DAU by region (input to the script).
- `prior_year_all.csv` — 2025 total daily DAU; calendar-aligned reference line that
  makes the forecast's forward decline read as a disconnect (top panel).
- `region_ma28.csv` — the plotted 28d-MA series (output).
- `pancake_desktop_dau.png` — the figure.

## Region definition
EU = IT+DE+FR+PL (EU members in the forecast market list). ROW = ALL − CN − US − EU,
so the four regions sum exactly to the world total. `legacy_desktop` 28d-MA = 48.9M
on 2026-06-27, which matches the dashboard's most-recent dot.

## What's NOT here
- glean_desktop / combined-source breakdown (this is legacy_desktop only).
- A true actual-vs-prior-forecast gap (the overlaid line is the current vintage's
  forward projection, not a vintage that predicted Jan–Jun).

## Where new code goes
Region-level desktop gap diagnostics for this question. Cross-source or
prior-vintage comparisons that grow beyond a single script should get their own
sibling dir or fold into `csv-vs-actuals/`.
