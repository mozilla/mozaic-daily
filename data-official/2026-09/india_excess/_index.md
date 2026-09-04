# `i` — india_excess, cycle 2026-09

India's 2026 desktop DAU above what a typical year would have done, as a `desktop_overlay`
for the daily forecast. Data edge **2026-08-29**. Produced by
`scratch/brwells/regional-story/india_forecast/deliver.py`; evidence page
`site/india_forecast.html`.

## Which file to use

* **`india_excess.json`** — the spec. Points at the **SETTLE** scenario.
* **`india_excess.{scenario}.2026-08-29.parquet`** — what the pipeline loads.
  `load_lift_series` reads `india_excess_dau_daily` off a DatetimeIndex, so the date must stay the
  index and never become a column.
* **`.csv`** — the same numbers for reading. **Not** loadable by the pipeline.
* **`.meta.json`** — provenance, the onset rule and its alternatives, the fitted half-life,
  and the caveats.

## Scenarios (net excess, trailing 28-day mean, DAU/day)

| scenario | Dec-15 2026 | Jun-15 2027 | Dec-15 2027 |
|---|--:|--:|--:|
| Hold · the peak excess is carried flat | 57,155 | 57,155 | 57,155 |
| Proportional · a constant share of the typical level | 41,945 | 41,099 | 41,945 |
| Linger · decays over a year | 33,317 | 20,115 | 12,110 |
| Settle · decays slowly | 20,946 | 5,156 | 1,260 |
| Fade · decays fast | 1,161 | 1 | 0 |

Switching scenario is a one-line edit to `data_file` in the spec **plus a model re-run** —
the curve is subtracted from training rows, so a spec-only change moves nothing downstream.

## Four things to know before using it

1. **It is a measured gap with a hypothesised cause.** The curve is 2026 minus the
   2022–2025 typical year. A university-calendar change is the leading
   explanation and is not established. Report it as "India above typical", not as an
   education effect.
2. **It is already net of `l`.** India carries 5.72% of the launch-on-login
   lift (11,450/day at the edge); that is subtracted here so the two
   overlays do not remove the same DAU twice. Do not net it again.
3. **It carries no holiday term, and it changes nothing about how mozaic sees holidays.**
   Every year was bridged before measuring — mozaic's calendar days that dipped, plus any
   India-only dip mozaic's calendar does not name (it has no Diwali or Dussehra) — so the
   gap is holiday-neutral. Left in, 2026's Saturday Independence Day alone would have
   inflated the August gap by ~1.7 index points. The delivered curve is `actual × ratio`,
   so every dip stays in the training frame exactly as it is today.
4. **The curve runs to 2027-12-31.** `add_lift_to_forecast` zero-fills absent dates;
   a curve ending at end-2026 would drop the whole component on 1 January. `fade` reaches
   zero on its own; the others are held at their terminal path.

## Not wired

`main.py` hand-wires each adjustment code. `i` still needs a
`_find_india_excess_spec_for_forecast` / `_apply_india_excess_pre_mozaic` pair (copy the
`o` pattern — fixed shares — with a distinct sentinel), an `add_lift_to_forecast` call, and
an entry in `data-official/adjustment_codes.yaml`. Single-letter codes only.
