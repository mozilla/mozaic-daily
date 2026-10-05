# `l` — launch at login for new users, cycle 2026-09

**Retitled 2026-09-04** from "launch-on-login": this code is the tailwind for the feature as shipped to **new
users** (experiment `long-term-holdback-2026-growth-desktop`, 100% rollout 2026-05-08). A second tailwind,
**launch at login for existing users**, is expected before the end of the September cycle and will be ingested as
its own code so the two can be sized and audited independently. Registry `name`, directory and spec filename are all `launch_at_login_new_users`; July's and August's builds keep their historical
`launch_on_login/lol.json` layout, which the registry lists as a legacy `spec_glob` so they still resolve.

**Carried forward unchanged from August.** The 200,000 DAU/day ceiling selected 2026-07-29, the curve
(`lol_tailwind.2026-07-29.cap200k.parquet`) and its model meta are byte-identical copies of
`../../2026-08/launch_on_login/` (the producer's file names, `lol_tailwind.*`, are kept as delivered); only `applies_to_forecast_start` moved to 2026-09-02. Nothing was re-measured:
the measurement window closed on 2026-06-23 when the holdback received the feature, so every cycle's curve is that
measurement extrapolated. See the August `_index.md` for the ceiling decision and the deleted alternates.

| file | role |
|---|---|
| `launch_at_login_new_users.json` | the spec, gated on `applies_to_forecast_start: 2026-09-02` |
| `lol_tailwind.2026-07-29.cap200k.parquet` | what the pipeline loads: `lol_lift_daily` on a `target_date` index (+ `lol_lift_ma`) |
| `lol_tailwind.2026-07-29.cap200k.model_meta.json` | the producer's meta: `cap_dau_daily` 200,000, measurement and extrapolation record |

Allocation: proportional to population (`trailing_dau_share` over 28 days of `modern_windows` DAU); no exclusions
(the spec has none; IR receives its population share as in August).

## Where new files go

A re-measured or re-capped curve: produce it with `~/work/launch-on-login/build_lol_tailwind.py`, drop it here, repoint
`data_file` / `model_meta_file`, delete the loser (build variants while deciding, keep one). The existing-users
tailwind goes in its own directory under its own code, not here.
