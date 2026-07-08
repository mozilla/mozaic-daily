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
- **`mozillaonline/`** — MozillaOnline (`o`) CN desktop migration overlay. **Wired** (adjustment
  code `o`, Brad's official model) and applied bidirectionally in the locked desktop model.
- **`marketing/`** — refreshed marketing-lift (`m`) model for mobile DAU.
- **`desktop_locked/`** — the **LOCKED desktop forecast** (legacy_desktop DAU, forecast_start
  2026-07-06): the parameter-search result `cps=0.08983 / cpr=0.65 / threshold=−0.032 (center)` with
  the `l` + `o` overlays. Dec-15 28d-MA = **48,585,483** (incl. the −1,345,000 Win10 headwind).
  Supersedes the desktop rows in the 06-29 combined `adj-lmo` parquet. See its `README.md`.
- **`mobile_refresh_2026-07-06/`** — the **refreshed standalone mobile forecast** (glean_mobile DAU,
  `grad_moderate` grid params + `adj-m`), retrained 2026-07-07 through the latest landed day
  (2026-07-05) at forecast_start 2026-07-06. Dec-15 28d-MA = **17,923,869**.
- **Both platforms now share forecast_start 2026-07-06** (consistent seam). The canonical notebook
  `july_canonical_v2026-06-29.ipynb` reads desktop from `desktop_locked/` and mobile from
  `mobile_refresh_2026-07-06/`; `new_profiles` is carried from the 06-29 combined parquet.
- **`csv/`** — public-facing canonical exports: `july_canonical_curves.csv` (daily 28d-MA — actuals +
  June prior + July current, per desktop/mobile/ALL) and `july_dec15_summary.csv`.
- **`adjustments/`** — headwind (`h`) spec (desktop −1,345,000 / mobile −27,162 at the Dec-15 anchor).
- **`iran_gap_holiday_mozaic_handoff.md`** — feature-request handoff to the
  `mozaic-forecasting` package: add a training-exclusion ("gap holiday") so the Iran
  internet-shutdown window is masked out of fitting instead of corrupting the trend.
  (Companion handoff for the telemetry opt-out investigation lives outside this repo at
  `~/work/experiments/telemetry-optout-dau-impact/HANDOFF.md`.)

## Open / follow-ups

- Re-unify a single combined `adj-lmo` parquet at forecast_start 2026-07-06 (desktop + mobile DAU +
  carried `new_profiles`) if a combined mart artifact is needed downstream; the canonical notebook
  currently reads the two per-platform parquets (`desktop_locked/`, `mobile_refresh_2026-07-06/`)
  directly.
- Per-country no-headwind CSVs (June had them under `csv/per_country/`) not yet regenerated for July.
- Archive this cycle to GCS (`july-2026/`) at button-down.

## Where new files go

Month-scoped artifacts (this cycle's producer/diagnostic notebooks, adjustment specs,
parquets, canonical CSVs) live here. Cross-month or topic-anchored work (mechanism
diagnostics, validation-against-actuals over time) goes to `research/{topic}/`.
