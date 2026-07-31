# `data-official/2026-08/marketing/` — the marketing team's paid-DAU curve

## Status: `m` RETIRED (2026-07-31). This curve is now consumed as a LEVEL by `p`.

**The `marketing.json` date gate is cleared**, so the `m` bidirectional overlay no longer fires. Mobile
now uses **`p` (`paid_organic_split`)** — see `../organic/_index.md`. This directory is still live,
because `p` reads the lift parquet as its **paid forecast**:

    paid(d) = marketing_lift_daily(d) + anchor_paid_dau (922,250.47)

**The anchor is load-bearing.** The delivered artifact is a *lift*, not a level, because that is what
`m` consumed. Getting the anchor wrong shifts every total by a constant while leaving the shape right,
so nothing downstream would catch it. It is pinned in `../organic/organic.json` and asserted by
`tests/test_organic.py::test_real_august_marketing_level_matches_the_published_anchor_and_lift`.

Also: `p` **holds the curve flat** past its last day (2026-12-31) through `forecast_end_date`
(2027-12-31). `m` zero-filled there, which put a −637K cliff on 2027-01-01; with a level it would have
been −1.56M.

### Why `m` was retired

Desktop's `l` and `o` can honestly model "incremental DAU since launch" because those interventions have
hard start dates. **Mobile paid acquisition has run continuously for years**, so the anchor was an
accounting choice — and the bidirectional overlay **absorbed 58%** of any change to the curve. The
absorption result below is the clearest evidence: two lift curves differing by −141,653 at Dec-15 moved
the KPI only −59,875. A lift-curve comparison was never a KPI estimate.

Mobile telemetry carries a client-level paid/organic flag, so under `p` paid is *measured* rather than
modelled, and it contributes **exactly its own value** with no Prophet interaction.

**Revert kit: `../mobile_adjm_REVERT_2026-07-31/REVERT.md`.** Reverting means restoring the marketing
date gate *and* clearing the organic one *and* repointing the notebook — three changes as one unit.
No model re-run needed; the `adj-m` parquet was never overwritten.

## History: the 2026-07-31 methodology swap (while `m` was still live)

`marketing.json` points at `marketing_lift_model.uac_meta_total.2026-07-28.parquet`. The canonical
mobile forecast was rebuilt on it and the canonical notebook reran; mobile Dec-15 28d-MA moved
**17,924,607 -> 17,864,732 (-59,875)**. Desktop is byte-identical (verified: 0 delta on all 365
curve days).

**That previous methodology is backed up at `../_backup_mobile_methodology_2026-07-31/` with restore
instructions in its `RESTORE.md`** — note this is the *`m`-era* backup, one swap earlier than the
`p` migration. Restoring needs no model re-run — the old build is still on disk.

The `m`-era description, kept because it explains the artifacts below: bidirectional overlay on
`glean_mobile` DAU — the daily paid-acquisition lift was subtracted from Fenix Android training rows
before mozaic, then added back to the per-tile forecast, allocated by each country's share of Fenix
Android DAU over a trailing 28-day window. (Note the `scope.exclude_countries: ["IR"]` key was declared
but **never read** by the `m` code path, so IR did silently receive a share of the lift. `p` honours it.)

| file | role |
|---|---|
| `marketing.json` | the spec — **wired to `uac_meta_total`**, `applies_to_forecast_start: 2026-07-28` |
| `marketing_lift_model.uac_meta_total.2026-07-28.parquet` | **WIRED** — Total Paid DAU basis |
| `marketing_lift_model.uac_meta_rolling.2026-07-28.parquet` | alternative — 12-mo rolling basis, not wired |
| `marketing_lift_model.total.2026-06-29.parquet` | July's curve, superseded (restore target) |
| `august_marketing_lift.ipynb` | the derivation + the test-apply |
| `source_data/uac_meta_paid_dau.20260730.csv` | query output, 52 weekly rows |
| `source_data/query_uac_meta_paid_dau.sql` | the query as run (template params resolved) |

## The source changed this cycle

The marketing team replaced the hand-delivered spreadsheet with a query over
`mozdata.analysis.ahe_cmo_dashboard_*` and `ahe_meta_android_*`. Two consequences:

1. **Two channels now.** Paid acquisition runs on UAC *and* Meta Android (from 2026-05-04, 8 countries).
   The query returns four presentation lines; the Meta pair is **cumulative** (`uac_v + meta_v`), so the
   combined series is `COALESCE(uac_meta_*, uac_*)` — *not* a sum of the pairs. The notebook asserts this
   on every overlapping week. Meta is fully incremental by decision and is 2.3% of combined at year end.
2. **No cohort dimension.** All three new tables carry only `week`, `country`, `paid_dau`. The old CSV had
   `Pre-2026 DAU` / `2026 DAU`. Both candidates below therefore understate the post-anchor cohort DAU the
   overlay contract specifies, by an amount this source cannot measure. **Worth asking the marketing team
   to expose a cohort dimension.**

## Method: anchor-and-subtract

    lift(d) = paid_dau(d) − paid_dau(2026-03-30),  0 before the anchor

Restores the method from `research/marketing-lift/v2-real-data/02_forecast_projection.ipynb`
(`[anchor-at-launch]`), which June replaced with an empirical-gap hybrid. Memoryless — each delivery
re-derives the whole curve, versus the June→July→August chain that stacked two cycles of outlook deltas
onto one 45-day measurement. Also smooth, where July's curve carries raw day-of-week noise into the
training rows it is subtracted from.

### Rejected alternatives

- **Anchored-delta variants** (`L_aug = L_jul + Δsource`) — carry July's stale curve forward. Superseded
  once the original anchor-and-subtract method was identified as what prior cycles were meant to be doing.
- **Empirical re-measure** — `lift = actuals − marketing-off counterfactual`. Rejected: any counterfactual
  must predate the 2026-04-06 launch, making it a 16-week extrapolation, so forecast error enters the
  estimate 1:1 and cannot be separated from lift. Corollary: notebook 02's `validation-c-counterfactual`
  residual (mean +22,710, max +127,027) was **never evidence against** anchor-and-subtract — a drifting
  counterfactual produces that signature even if the method is exact.

## The metric basis: why Total, not 12-mo rolling

`L_rolling < L_total`. The gap is paid DAU from cohorts older than 12 months, which *grows* (193K at the
anchor to 345K at Dec-15) as 2025 and early-2026 cohorts cross the boundary. That crossing is an
accounting artifact — those users did not leave, they stopped being counted — so the rolling basis
discards a real signal for a bookkeeping reason. Total is wired on that basis, and because it is what
the original method used.

| candidate | Dec-15 lift | vs July's 778,880 | % Fenix DAU | status |
|---|--:|--:|--:|---|
| `uac_meta_total` | 637,227 | −141,653 | 5.1% | **wired** |
| `uac_meta_rolling` | 485,544 | −293,336 | 3.9% | built, not wired |

Both still understate the post-anchor cohort DAU the overlay contract specifies, by an amount this
source cannot measure — hence the cohort-dimension ask above.

## Plots

In `../plots/`: `marketing_source_uac_meta.png` (the four query lines + Meta band),
`marketing_method_d_candidates.png` (both candidates vs July's curve, daily and 28d MA),
`marketing_basis_decomposition.png` (what separates the two bases).

## Realised KPI effect vs the curve difference

The two lift curves differ by **-141,653** at Dec-15, but the mobile KPI moved only **-59,875** — the
bidirectional overlay absorbs **58%**, because a smaller subtraction from training leaves Prophet a
higher organic trend that partly offsets the smaller add-back. The absorption strengthens with
horizon: the gap peaks near -125,000 in early September and closes to -59,875 by Dec-15.

**Consequence: a lift-curve comparison is not a KPI estimate.** The ~152,000 spread between the two
metric bases would land closer to ~64,000 at Dec-15. See `../plots/marketing_testapply_kpi.png` and
the `[plot-testapply]` cell.

## Builds

| build | lift curve | mobile Dec-15 28d-MA |
|---|---|--:|
| `../mobile_uac_meta_2026-07-28/` | `uac_meta_total` (current) | 17,864,732 |
| `../mobile_baseline_2026-07-28/` | July carried-forward (superseded) | 17,924,607 |

Both remain on disk. The canonical notebook's `[mobile-dec15]` pins the current value at 17,864,732
and asserts it, so an accidental mobile rebuild during desktop-only work still fails loudly.

**Where new files go:** a rolling-basis build if that variant is ever adopted, and any re-measure once
the marketing team exposes a cohort dimension.
