# Questions for the marketing analyst

Open questions on `paid_dau_analysis_20260519 — weekly_dau_forecast.csv` (the `total_paid_dau` / `baseline_dau` weekly forecast). Collected here while the analyst is offline so we can keep working.

Last updated: 2026-05-19

## Highest priority — affects anchor choice

**1. Is "campaign start = 2026-04-06" the right interpretive anchor for the lift series?**
The CSV's `total_paid_dau` rises steadily from ~616k (2026-01-05) to ~1,014k (2026-04-06) — a ~30k/week ramp pre-launch — and continues climbing post-launch. If we treat the campaign as a discrete event starting 2026-04-06, our derived lift explains only ~43% of the empirical (June actuals − April mktg-off forecast) gap on Fenix Android. If we anchor 6 weeks earlier (around 2026-02-23), the lift explains ~100% of the gap. Which framing matches the analyst's intent?
- **Option A (discrete launch):** pre-launch growth is unrelated to this campaign — it's other ongoing paid acquisition. Anchor at 2026-04-06. Residual mean: −143k.
- **Option B (soft launch):** the campaign ramped up in late March / early April. Anchor at 2026-03-30 or 2026-04-01. Residual mean: −110k to −120k.
- **Option C (continuous attribution):** the whole pre-launch ramp is part of the same paid-acquisition channel; the April launch accelerated an existing trajectory. Anchor at 2026-02-23. Residual mean: +17k.

**Interim choice while waiting:** Option B with anchor=2026-03-30. Re-anchorable in 5 minutes once we hear back.

## Medium priority — affects magnitude interpretation

**2. What `normalized_channel` filter does the underlying model use?**
DS team's STMO query 118452 (the closest published view of the same signal) has `normalized_channel = 'release'` **commented out**, so it includes all channels (release + beta + nightly). The convolution model in `.claude/worktrees/marketing-lift/` filtered to release only. We don't know which the marketing-team CSV uses. If the CSV includes non-release channels, that's a known contributor to magnitudes diverging from the convolution model.

**3. Does the CSV exclude Iran (`fa` locale + IR country)?**
We've assumed yes (matching the convolution-model baseline). If no, magnitudes would be inflated by Iran's contribution to the Paid cohort. Validation-A residual will surface this if it's wrong, but it'd be cleaner to confirm explicitly.

**4. How does the marketing-team model define "Paid DAU"?**
Three definitions are in play in our work right now:
- The marketing-team CSV (this artifact's source) — uses some attribution definition we don't have visibility into
- DS team STMO 118452 — cohort-based: any client ever paid-acquired via Play Store + gclid → all future DAU rows count as Paid
- `*_marketing_geo_testing_v1` tables — third, separate definition, flagged as wrong by the user

Does the marketing-team CSV match STMO 118452's cohort definition, or does it use a different (perhaps narrower) one? If narrower, it would explain under-counting relative to the empirical gap.

## Lower priority — affects forward projection

**5. What's the basis for the post-2026-12-28 trajectory?**
The CSV ends 2026-12-28. We forward-fill the last 3 days through 2026-12-31. If the marketing team's projection extends into 2027, we could ship a longer series. Otherwise the existing `m` adjustment's "campaign winds down in 2027" behavior kicks in. Is that intended?

**6. Will there be a refreshed CSV at the next monthly forecast cycle?**
Country shares in the existing `m` adjustment are frozen at training-end-date (per `marketing.json`). If the marketing-team forecast is re-run monthly, we should plan for a periodic re-promotion. What's the cadence?
