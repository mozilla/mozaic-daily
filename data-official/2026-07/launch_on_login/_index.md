# launch_on_login/ — July 2026 LOL desktop tailwind (adjustment `l`)

The launch-on-login (LOL) feature (Firefox launches at OS login for new modern-Windows
installs; experiment `long-term-holdback-2026-growth-desktop`, 100% rollout 2026-05-08) added
incremental desktop DAU. This directory holds the spec + curve that wire it into the July
`legacy_desktop` DAU forecast as a **bidirectional overlay** (adjustment code `l`), applied with
the same subtract-then-add machinery as marketing-lift `m`.

## What's here
- `lol.json` — the `desktop_overlay` adjustment spec (loaded by `mozaic_daily.adjustments.load_overlay_spec`).
  Allocation keys off the boolean `modern_windows` segment column, trailing-28d DAU country share.
- `lol_tailwind.2026-06-29.parquet` — the daily lift curve (`lol_lift_daily`, `lol_lift_ma`),
  DatetimeIndex `target_date`, spanning 2026-01-01 → 2027-12-31.
- `lol_tailwind.2026-06-29.model_meta.json` — producer provenance (cap, clean window,
  contamination date, source sha1s, git commit).

## Key facts
- **Cap = 125,000 DAU, flat, daily space** (→ Dec-15 desktop 28d-MA contribution ≈ +125K).
  Deliberately conservative — the measured excess had not plateaued (still rising ~19K/wk).
- **Clean window ≤ 2026-06-23**; contamination begins **2026-06-24** (holdback control receives
  the feature; excess collapses spuriously). Curve rises with the measured 7d-MA and clamps at
  125K from 2026-06-19, then holds flat to the forecast horizon.
- **Scope:** `legacy_desktop` DAU, `modern_windows` segment only.

## Producer
`~/work/launch-on-login/build_lol_tailwind.py` (reads the experiment's cached
`tmp/obs_dau.parquet` + `tmp/obs_enr.parquet`). On regeneration, copy the parquet + meta here and
bump `applies_to_forecast_start` / `data_file` in `lol.json`.

## Where new code goes
Curve/producer changes live in the launch-on-login repo. Applier changes live in
`src/mozaic_daily/adjustments.py` (generic `subtract_lift_from_training` / `add_lift_to_forecast`,
shared with `m` and the forthcoming MozillaOnline `o` overlay).
