# `data-official/2026-07/` — July 2026 forecast cycle

Active cycle (branch `july-forecast`, off `june-forecast`).

## What's here

- **`TODO_factors.md`** — the cycle's planning doc: every factor under consideration
  (Iran return + gap holiday, MozillaOnline migration overlay, Win10 headwind resize,
  telemetry opt-out via deletion requests, desktop/mobile marketing, usage-experiment
  DAU movement), its modeling approach, the data needed, status, and the open questions
  for the user. **Start here.**
- **`launch_on_login/`** — the launch-on-login (`l`) desktop DAU tailwind: `lol.json`
  spec + `lol_tailwind.*.parquet` curve. Bidirectional overlay on `legacy_desktop` DAU,
  125K flat conservative cap. See its `_index.md`.
- **`mozillaonline/`** — MozillaOnline (`o`) CN desktop migration overlay. Model artifact
  done; wiring pending — `WIRING_HANDOFF.md` has step-by-step reuse of the `l` machinery.
- **`marketing/`** — refreshed marketing-lift (`m`) model for mobile DAU.
- **`adjustments/`** — headwind (`h`) spec.
- **`iran_gap_holiday_mozaic_handoff.md`** — feature-request handoff to the
  `mozaic-forecasting` package: add a training-exclusion ("gap holiday") so the Iran
  internet-shutdown window is masked out of fitting instead of corrupting the trend.
  (Companion handoff for the telemetry opt-out investigation lives outside this repo at
  `~/work/experiments/telemetry-optout-dau-impact/HANDOFF.md`.)

## What's not here yet

- `desktop_<slug>/` per-config parquets, `parameters.json`, and `csv/` canonical exports.
- The **re-run `legacy_desktop` DAU** carrying the `l` (and forthcoming `o`) overlays, swapped
  into the canonical combined parquet. Per Brendan, regenerate the canonical desktop **once** with
  both `l` and `o` (see `mozillaonline/WIRING_HANDOFF.md`), not once per overlay.

## Where new files go

Month-scoped artifacts (this cycle's producer/diagnostic notebooks, adjustment specs,
parquets, canonical CSVs) live here. Cross-month or topic-anchored work (mechanism
diagnostics, validation-against-actuals over time) goes to `research/{topic}/`.
