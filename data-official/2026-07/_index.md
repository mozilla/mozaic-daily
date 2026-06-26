# `data-official/2026-07/` — July 2026 forecast cycle

Active cycle (branch `july-forecast`, off `june-forecast`). Setup in progress —
the forecast has not yet been run.

## What's here

- **`TODO_factors.md`** — the cycle's planning doc: every factor under consideration
  (Iran return + gap holiday, MozillaOnline migration overlay, Win10 headwind resize,
  telemetry opt-out via deletion requests, desktop/mobile marketing, usage-experiment
  DAU movement), its modeling approach, the data needed, status, and the open questions
  for the user. **Start here.**
- **`iran_gap_holiday_mozaic_handoff.md`** — feature-request handoff to the
  `mozaic-forecasting` package: add a training-exclusion ("gap holiday") so the Iran
  internet-shutdown window is masked out of fitting instead of corrupting the trend.
  (Companion handoff for the telemetry opt-out investigation lives outside this repo at
  `~/work/experiments/telemetry-optout-dau-impact/HANDOFF.md`.)

## What's not here yet (added during the run, per monthly-forecast-update skill)

- `adjustments/` — headwind + any new component specs for July
- `marketing/` — refreshed marketing-lift model
- `desktop_<slug>/`, `mobile_<slug>/` — per-config forecast parquets + sidecars
- `parameters.json`, the producer notebook, and `csv/` canonical exports

## Where new files go

Month-scoped artifacts (this cycle's producer/diagnostic notebooks, adjustment specs,
parquets, canonical CSVs) live here. Cross-month or topic-anchored work (mechanism
diagnostics, validation-against-actuals over time) goes to `research/{topic}/`.
