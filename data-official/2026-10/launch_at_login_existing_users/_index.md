# `e` — launch_at_login_existing_users, cycle 2026-09

<!-- Drafted by scripts/ingest_adjustment.py on 2026-09-10. Fill in the WHAT/WHY sections. -->

**What it is:** Desktop DAU added by Launch at Login for *existing* users, the second half of the feature whose
new-users half is code `l`. Delivered 2026-09-10 as `launch-on-login-dau-forecasts.csv` (three-column daily contract).
The measured part is the incremental DAU since the existing-users rollout began 2026-07-30, through 2026-09-08 (the day
before the seam); the rest is the producer's forecast, which steps with the **planned** rollout — about 66K/day from
the seam, about 77K on 2026-10-13, about 736K from 2026-10-15 — and eases to 669K by Dec-31. It enters the forecast
the way `l` and `o` do: subtracted from `legacy_desktop` modern_windows training rows before mozaic so Prophet does not
extrapolate the ramp, then added back after. Allocation is **localized by fixed shares** measured once from BigQuery
(`source_data/en_locale_win1x_country_shares.sql`, 69.8 GB): each country's share of trailing-28d legacy-desktop DAU
on Windows 10/11 in an `en*` locale over 2026-08-12..2026-09-08, because the feature ships to en locales. IR (3.7% of
that population) is excluded and the rest renormalized.

**WITHHELD.** The spec carries `"withheld": true`: the code is registered and gated on the 2026-09-09 seam, the run log
names it as WITHHELD, and it is **not applied** — output markers do not carry `e`. To turn it on, delete that key (or
set it false) and rerun the desktop model; to turn it back off, put it back. This is the on/off switch the user asked
for on 2026-09-10; `--disable-adjustment e` is the one-off-run override and is not needed while the key is present.

**Family:** per-tile overlay: subtracted from training rows before mozaic and added back after; **needs a model re-run**. **Platform:** desktop (`legacy_desktop`). **Sign:** tailwind (+).

## Files

| file | role |
|---|---|
| `launch_at_login_existing_users/launch_at_login_existing_users.json` | the spec, gated on `applies_to_forecast_start: 2026-09-09` |
| `launch_at_login_existing_users.2026-09-08.parquet` | what the pipeline loads: `launch_at_login_existing_users_dau_daily` on a `target_date` DatetimeIndex, `launch_at_login_existing_users_dau_ma`, `source` |
| `launch_at_login_existing_users.2026-09-08.meta.json` | provenance: source sha1, column mapping, coverage, hold-flat rule, checks |
| `source_data/launch-on-login-dau-forecasts.csv` | the delivered file, byte for byte |
| `plots/launch_at_login_existing_users.2026-09-08.curve.png` | the curve's shape: daily + 28d mean, measured / projected / held, seam and Dec-15 marked |
| `interrogate_existing_users_model.ipynb` | interactive interrogation of the delivered model (opened 2026-09-11): raw daily curve, weekly means, step sizes; comparison inputs (experiment data, September forecast, actuals) are added as sections as they arrive |
| `plots/interrogate_raw_delivered_curve.png` | the notebook's raw-data chart: full horizon on a log axis + linear zoom on the measured ramp and seam hand-off |

## Coverage

| | |
|---|---|
| delivered | 2026-07-30 → 2026-12-31 |
| actuals through | 2026-09-08 |
| held flat from | 2027-01-01 at 723,853/day (mean of the final 28 delivered daily values) |
| horizon | 2026-01-01 → 2027-12-31 |
| Dec-15 28d MA | 736,935 |

## Allocation

Localized: fixed country shares {"ROW": 0.373138, "US": 0.336728, "IN": 0.135718, "ID": 0.058009, "CA": 0.045854, "DE": 0.015865, "FR": 0.007131, "BR": 0.006072, "PL": 0.0056, "MX": 0.004162, "IT": 0.00383, "JP": 0.002466, "RU": 0.002386, "CN": 0.001658, "AR": 0.001382}; excluded: ['IR'].

## What is measured and what is assumed

| stretch | basis |
|---|---|
| 2026-07-30 → 2026-09-08 | **measured** daily incremental DAU (producer's telemetry measurement; unsmoothed, weekday swing kept) |
| 2026-09-09 → 2026-10-14 | producer's **model**, about 66K → 77K/day; the first forecast day sits ~21K above the last measured day, a seam step inside the delivered file |
| 2026-10-13 and 2026-10-15 steps | **planned rollout** dates and populations (user-confirmed 2026-09-10); not observed |
| 2026-10-15 → 2026-12-31 | producer's model on the full population: ~735K plateau, Dec-15 737,537 daily / 736,935 28d-MA, easing to 669K by Dec-31 |
| 2027 | **held flat** at 723,853/day (mean of the last 28 delivered values), our convention so the component does not vanish on 1 January |
| country split | **measured once** (fixed shares, en-locale Win10/11 DAU), not re-measured per run; ROW 37.3%, US 33.7%, IN 13.6%, ID 5.8%, CA 4.6% |

## Where new files go

A refreshed curve for this cycle: re-run the ingest with `--replace`; the previous build moves to `launch_at_login_existing_users_REVERT_<date>/`. Cross-cycle analysis of this effect goes to `research/`.
