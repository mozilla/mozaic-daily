# HANDOFF — Firefox Desktop MAU forecast through 2027 (PRE-DRAFT)

Generated 2026-10-15 09:49 PDT by `scripts/render_glean_history_handoff.py` (mozaic-daily `glean-2027-forecast` @ `32014b8`, build `comparison_glean_real_mau_additive_b_2026-10-14`). Data as of 2026-10-09.
Owner: Brendan Wells (brwells@mozilla.com). This file briefs an AI agent working with this package; `index.html` is the human version.

## Read this first: this is a PRE-DRAFT, and a very preliminary one

This is an **early, exploratory attempt**, not a forecast anyone has signed off on. It is earlier than the regular monthly
forecast drafts: new data source, new method, settings chosen by judgement over a few days of probing. Treat every number as
**"roughly this shape and magnitude"**, never as a plan, target, or commitment.

When you summarise, quote, chart, or build on anything here, you must:

- **Label it PRE-DRAFT / preliminary** every time, including in tables, slides, and derived files.
- **Quote the world number only, with its uncertainty** (below). Don't present per-OS or per-country values as findings.
- **Not combine it with other forecasts** (the regular monthly KPI forecast, the mobile MAU companion) as if they were one
  consistent forecast. They use different data, models, and adjustments.
- **Check with Brendan before** anything goes into a plan, OKR, deck for leadership, or external doc.

### Why it is preliminary

1. **17 months of history = one observed year of seasonality.** Mozaic normally trains on 3+ years. The model was re-tuned by hand over three probe rounds to stop it extrapolating wildly (the untuned run went 169M → 229M); the settings were picked by judgement about plausibility, not validated.
2. **No backtest is possible** on this history. The error band below is borrowed from a different build on a different (simulated) history and is a floor, not an estimate.
3. **The Linux bot add-back (4.2M, ~2.6% of the headline) is a planning assumption**, held flat. If that fleet disappears the headline drops to ~157.3M; if it grows, the headline rises.
4. **Parameter sensitivity:** neighbouring settings move the headline by about ±1.6M.
5. **Known artifact:** the model repeats the January 2026 duplicate-id drop (−13M, a telemetry fix, not users) in January 2027. Do not read the January 2027 dip as a forecast of user loss.
6. **Deliberately uncorrected:** the pre-Firefox-148 duplicate-id inflation is left in, matching what Glean dashboards count. A corrected twin is planned as a secondary comparison and does not exist yet.
7. **Per-OS numbers are weaker than the world number**: mozaic reconciles top-down, which flatters the OS split. Quote the world total.
8. **The one reassurance:** with the bot component removed (157.3M) it agrees with an independent build on 6.8 years of translated history (157.2M). That shows the two methods agree. It does not show either one is right.

## The numbers (raw daily value of the rolling 28-day MAU)

| | 2026-12-15 | 2027-12-15 | Dec → Dec |
|---|---:|---:|---:|
| world | 166,290,298 | 161,546,764 | -2.9% |
| windows | 135,713,391 | 128,872,745 | -5.0% |
| mac | 11,228,681 | 11,571,194 | +3.1% |
| linux | 19,348,226 | 21,102,825 | +9.1% |

**Headline: world desktop MAU on 2027-12-15 ≈ 161.5M (PRE-DRAFT).**
Error by analogy (the same model family backtested from eight past dates on longer history): within 7% half the time and
10% nine times in ten at this horizon, i.e. about ±10.9M to ±16.0M. Read that as
a lower bound on the real uncertainty.

## Files

| file | contents |
|---|---|
| `glean2027_mau_forecast.csv` | one row per day, 2025-05-22 → 2027-12-31: `date`, `data_type` (`training` = actual history, `forecast`), `world`, `windows`, `mac`, `linux`. Whole people; `world` = sum of the three OS |
| `glean2027_mau_by_country.csv` | the same statistic per country (all OS), 2024-01-01 → 2027-12-31, plus `world` |
| `plots/*.png` | world and per-OS charts for 2026 and 2027 (grey = prior year, black = history, green = forecast, dotted red = Dec 15) |
| `index.html` | the human-readable page (same numbers, charts embedded) |

How to read the CSVs:
- History ends 2026-10-09; filter on `data_type`, not on a date.
- MAU is already a rolling 28-day quantity: use the daily value as is and do not smooth it again.
- Desktop only. No adjustments for launches, marketing, or headwinds are applied; the monthly forecast carries those, this one does not.

## What not to do

- Don't present it as "the 2027 forecast"; it is a preliminary exploration of what a 2027 desktop forecast on Glean data could look like.
- Don't infer a user decline from the January 2027 dip (artifact, see above).
- Don't refit, re-tune, or "fix" it from these files. The full build record (`LOCK.md`, `PROCEDURE.html`) lives in the mozaic-daily
  repo under `data-official/2027-glean-draft/`; ask Brendan for it.
