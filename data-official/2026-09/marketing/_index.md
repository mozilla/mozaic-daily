# `data-official/2026-09/marketing/` — the paid-DAU curve `p` consumes (September 2026)

**Status: REPOINTED 2026-09-09 to the `pull2026-09-09` curve** (see *Pulls on disk*). The sections below describe the
2026-09-04 build, which stays on disk as a sibling; the method is identical. `../organic/organic.json` points `paid_forecast` at the 09-09 parquet's
`paid_dau_level_daily` column; the split was rebuilt for the September window the same day (see `../organic/`).

**Framing change 2026-09-09.** `p` now reads the level column **as delivered** and `organic.json` carries **no anchor**. Both
pulls on disk also carry `marketing_lift_daily` (= level − level on 2026-03-30) because they were built before the change;
those columns are inert. The paragraph below that says the framing "is not to be dropped" recorded the reasoning at the
time; it was dropped because the round-trip cancels exactly in the forecast region (`p` uses *measured* paid for training
rows, so the marketing file's framing never touched history) and its only effect was a float copied by hand after every
re-pull. `scripts/pull_paid_dau_curve.py` writes `paid_dau_level_daily` + `paid_dau_level_ma` only from now on.
The "Wiring" section below is kept as the procedure for the next re-pull, updated for the new framing.

`m` (marketing_lift) stays retired; there is no `marketing.json` here. This directory holds the paid **level**
input to `p`.

## What it is

The marketing team's paid mobile DAU (UAC + Meta Android) from the new **GMIO cross-channel feed**
(`ahe_gmio_weekly_paid_dau_views_20260901`), which replaced the two single-channel feeds August read. The
query is the team's widget query with the two template params resolved (metric = 'Total Paid DAU',
country = 'All'); it returns four presentation lines (UAC actual / forecast, UAC+Meta actual / forecast, Meta
stacked cumulatively on UAC). **Composition rule (Brendan, 2026-09-04): where the UAC+Meta line is present use
it, otherwise the UAC-only line — for actuals and forecast alike.** Every value used is one of the four query
columns verbatim; the workbook names which.

**The numbers moved, and the feed says the move is real**: future UAC spend went from $4.75M over 19 weeks to
$6.14M over 18 weeks (+36%/week) and the curves were refit (GMIO run 2026-08-28). The definitions did not
change. Elapsed weeks moved 3–6% (actuals revision); the rest is plan and refit.

## Files

| file | role |
|---|---|
| `build_paid_dau_curve.py` | the producer (reproducible, not throwaway): compose → interpolate → anchor-and-subtract → parquet + meta + workbook + plot |
| `source_data/query_gmio_paid_dau_total_all.sql` | the query as run (template params resolved) |
| `source_data/gmio_paid_dau_total_all.20260904.csv` | the raw query output, 51 weekly ISO-Monday rows, **the source of truth** (sha1 in the meta) |
| `paid_dau_curve.2026-09-02.xlsx` | three sheets: `raw_query`, `composed_weekly` (`paid_dau_used` + `basis` = which query column), `daily` |
| `marketing_lift_model.gmio_uac_meta_total.2026-09-02.parquet` | what `p` will load: `marketing_lift_daily` (lift vs anchor), `marketing_lift_ma`, `paid_dau_level_daily` (the level, for inspection) |
| `marketing_lift_model.gmio_uac_meta_total.2026-09-02.meta.json` | provenance + `key_values` (its `anchor_paid_dau` is no longer read by anything) |
| `plots/paid_dau_curve.2026-09-02.png` | level and lift, weekly points (filled = actual, hollow = forecast), August's Dec-15 level for reference |

## Method (August's, unchanged)

Each weekly value sits on its ISO Monday; linear interpolation to daily; forward-fill after the last Monday
(2026-12-21) to 2026-12-31; `p` then holds flat through 2027 (`tail_policy: hold_last`). Lift is
`level(d) − level(2026-03-30)`, zero before the anchor, so the parquet stays a lift and `p` adds the anchor
back — **the anchor is load-bearing and is now 800,831, not August's 922,250**. The
lift-plus-anchor framing is kept deliberately: it was August's answer to organic + paid not reproducing
history, and it is not to be dropped until that is confirmed unnecessary.

## Numbers

| | August (`uac_meta_total.2026-07-28`) | September (this curve) |
|---|--:|--:|
| anchor level at 2026-03-30 | 922,250 | 800,831 |
| level at the seam (2026-09-02) | — | 1,633,937 |
| lift at Dec-15 | 637,227 | 1,090,171 |
| **level at Dec-15** | **1,559,477** | **1,891,002** |
| level at Dec-31 | 1,563,950 | 1,904,795 |

Dec-15 paid level change vs August: **+331,525**. Because `p` stacks the level additively after
mozaic, that is the expected change in the published mobile Dec-15 from this input alone — but it lands only after
the split is rebuilt and the mobile model rerun.

Actuals run through the week of 2026-08-24 in the feed; the seam is 2026-09-02, so the seam-day value is the
feed's own forecast. `p` uses measured paid for training rows and this level from the seam on; the seam step
(`paid_seam_step`) must be re-measured after the rerun.

## Wiring (to do)

1. Fetch the September raw mobile pull: `python scripts/fetch_raw_pull.py` for `glean_mobile` DAU at seam 2026-09-02
   → `../mobile_rawpull_2026-09-02/`.
2. Rebuild the split: `python scripts/build_fenix_organic_split.py --forecast-start-date 2026-09-02 --production-raw <that pull>`
   (~141 GB scan). Writes `../organic/fenix_paid_organic.<T-0>.parquet` + sidecar.
3. Point `../organic/organic.json` `paid_forecast.data_file` at the new parquet with `value_column: paid_dau_level_daily`.
   No anchor key.
4. Extend `tests/test_organic.py` with a pin on the new Dec-15 level.
5. Mobile model rerun.

## Where new files go

A re-pull of the feed goes through **`scripts/pull_paid_dau_curve.py`** (skill `/pull-marketing-curve`), not this
directory's `build_paid_dau_curve.py`, which is the frozen 2026-09-04 producer. The script writes a **pull-date-suffixed
sibling** (`marketing_lift_model.gmio_uac_meta_total.2026-09-02.pull<date>.*`), saves the query output verbatim under
`source_data/`, and leaves `PENDING_WIRING.md`; it never edits `organic.json`. Wiring (repoint `paid_forecast.data_file`,
pin a test, rerun) is a separate step; there is no anchor to copy. Alternative bases (e.g. the 12-month-rolling view) go here too, named by basis.

## Pulls on disk

| pull | source table suffix | actuals through week of | anchor | level Dec-15 | status |
|---|---|---|--:|--:|---|
| 2026-09-04 (`…2026-09-02.parquet`, built by `build_paid_dau_curve.py`) | `_20260901` | 2026-08-24 | 800,831 | 1,891,002 | superseded 2026-09-09, kept as sibling |
| 2026-09-09 (`…2026-09-02.pull2026-09-09.parquet`, `pull_paid_dau_curve.py`) | `_20260909` | 2026-08-31 | 808,398 | 1,883,182 | **wired** in `../organic/organic.json` 2026-09-09; Dec-15 −7,820 vs the 09-04 pull; mobile rerun done 2026-09-09 → `../mobile_cpr0725_paid0909_2026-09-02/` |
