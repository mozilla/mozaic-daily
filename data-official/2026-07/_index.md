# `data-official/2026-07/` — July 2026 forecast cycle

Active cycle (branch `july-forecast`, off `june-forecast`).

## ✅ Current usable working set (the canonical July outputs)

These are the up-to-date, load-bearing files. Everything else in this cycle is an intermediate,
duplicate, or diagnostic (see "Present vs Archived" at the bottom):

- **Desktop forecast** — `desktop_locked/mozaic_daily_forecast.2026-07-06.ld-D.adj-lo.parquet`
  (+ sidecar, `parameters.json`, `README.md`). Locked model, `l`+`o` overlays, pre-headwind.
- **Mobile forecast** — `mobile_refresh_2026-07-06/cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6/mozaic_daily_forecast.2026-07-06.gm-D.adj-m.parquet`
  (+ sidecar, `parameters.json`).
- **Canonical curves / headline** — `csv/july_canonical_curves.csv`, `csv/july_dec15_summary.csv`,
  `kpi_sheet/official_forecast_data.2026-07-06.csv`.
- **Adjustment specs (wired)** — `adjustments/headwind.json` (`h`), `launch_on_login/lol.json` (`l`),
  `mozillaonline/mozillaonline.json` (`o`), `marketing/marketing.json` (`m`, → wired lift model
  `marketing/marketing_lift_model.total.2026-06-29.*`).
- **Iran fill (wired, package-side copy is authoritative)** — `iran_fill/iran_fill.*.parquet` + specs.
- **Producers** — `july_canonical_v2026-06-29.ipynb`, `regenerate_canonical_forecast.py`;
  review notebook `canonical_review_2026-07-06.ipynb`.

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
- **`iran_gap_holiday_mozaic_handoff.md`** — *historical* handoff. The shutdown gap is **not**
  handled by a NaN-mask "gap holiday"; the shipped mechanism is a **counterfactual fill** (train on a
  synthetic "what Iran would have been with no shutdown" series, real telemetry kept as actuals),
  which ships inside the mozaic package and auto-applies via `populate_tiles(data_source=...)`. See
  `TODO_factors.md` §0 and `iran_fill/`. (This doc's original gap-holiday framing was superseded.
  Companion telemetry-opt-out handoff lives outside the repo at
  `~/work/experiments/telemetry-optout-dau-impact/HANDOFF.md`.)

## Open / follow-ups

- Re-unify a single combined `adj-lmo` parquet at forecast_start 2026-07-06 (desktop + mobile DAU +
  carried `new_profiles`) if a combined mart artifact is needed downstream; the canonical notebook
  currently reads the two per-platform parquets (`desktop_locked/`, `mobile_refresh_2026-07-06/`)
  directly.
- Per-country no-headwind CSVs (June had them under `csv/per_country/`) not yet regenerated for July.
- Archive this cycle to GCS (`july-2026/`) at button-down. **Done** (2026-07-08).

## Present vs Archived

Cycle archived to `gs://…/july-2026/data-official/2026-07/` at button-down; the full tree also
remains in the `july-forecast` branch.

- **Present (on disk):** the canonical working set listed at the top (desktop_locked +
  mobile_refresh forecast parquets + sidecars, `csv/`, `kpi_sheet/`, adjustment specs, `iran_fill/`
  specs, canonical/review notebooks) plus the small `2026-06-29.gm+ld-D+NP.*` combined parquets
  (kept — they are the **new_profiles source** the canonical notebook carries) and the wired +
  candidate `marketing/` lift parquets.
- **Archived to GCS, removed from disk:** `desktop_lo_rerun/` (superseded pre-lock desktop rerun,
  669M pkl), `mobile_refresh_2026-07-06/…/mozaic_objects.*.pkl` (872M) + its raw parts parquet,
  `iran_fill/_draft/`, and the repo-root `2026-06-29` intermediates (`july-2026/root_intermediates_2026-06-29/`).

## Where new files go

Month-scoped artifacts (this cycle's producer/diagnostic notebooks, adjustment specs,
parquets, canonical CSVs) live here. Cross-month or topic-anchored work (mechanism
diagnostics, validation-against-actuals over time) goes to `research/{topic}/`.
