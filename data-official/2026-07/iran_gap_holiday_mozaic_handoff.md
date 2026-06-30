# Handoff to `mozilla/mozaic-forecasting` — ingest an Iran counterfactual *fill* for the shutdown gap

**From:** Brendan Wells (Firefox DAU forecasting) · **Updated:** 2026-06-30
**Context repo:** `mozaic-daily` branch `july-forecast`

> **⚠️ Supersedes the earlier "training-exclusion / gap-holiday (NaN-mask)" proposal.**
> An earlier draft of this handoff proposed masking the shutdown window out of training so Prophet
> would *interpolate* across it. **That approach was not adopted.** Instead we **fill** the gap with
> a synthetic counterfactual ("what Iran would have been with no shutdown") and feed it to mozaic as
> ordinary training data. The filename is kept for history; the content below is the live plan.
> (Why fill, not mask: a fill gives an explicit, inspectable, seasonally-correct series — including
> Nowruz — and needs no Prophet-interpolation behavior over an 86-day hole.)

## The situation

Iran's internet shutdown collapsed native IR telemetry to near-zero from **2026-03-01 → 2026-05-25**
(~86 days); it fully recovered **2026-05-26** to pre-shutdown levels. For July, IR returns to native
queries. Fed raw, the hole corrupts Prophet (spurious changepoints/trend, broken reconciliation).

mozaic already declares this window: `IranHolidays` marks **2026-02-28 → 2026-05-25** as
`"Internet Shutdown"` (`holiday_smart.py:322-327`). That excludes the window from holiday-effect
fitting but does **not** supply values and its detrend radius can't bridge 86 days — so a fill is
still needed.

## What mozaic-daily produces (done)

A **counterfactual fill** built by propagating the mozaic model forward (train on clean pre-shutdown
IR, forecast the gap), harvested per-tile, then re-seasonalized to restore the real weekday→weekend
amplitude Prophet damps. Artifacts + the consumption contract:

- `data-official/2026-07/iran_fill/iran_fill.{glean_desktop,legacy_desktop,glean_mobile}.parquet`
- **`data-official/2026-07/iran_fill/FILL_FORMAT_SPEC.md`** — the schema/contract (read this).
- Producer: `mozaic-daily/scripts/generate_iran_fill.py`.

## The ask for `mozaic-forecasting` (Approach A — pre-process the input dataframe)

No new mozaic *modeling* feature is required. The package just needs to **substitute the fill for the
real IR gap rows before `populate_tiles()`**. Per (data_source, metric) dataset, with IR no longer
excluded from the query:

1. Drop real IR rows whose date is inside that metric's fill window (DAU/NP/EED: 2026-02-28 →
   2026-05-25; **MAU: 2026-02-28 → 2026-06-21**, extended because rolling-28 MAU stays contaminated
   ~28d past recovery).
2. Append the matching fill rows from `iran_fill.<data_source>.parquet` (filtered to the metric).
   The fill carries the same columns (`x, country, <segment booleans>, y`) — a column-aligned concat.
3. Leave non-IR rows and out-of-window IR rows untouched.

See `FILL_FORMAT_SPEC.md` for exact columns, dtypes, populations, fill windows, and the
`ALL == sum(populations)` semantics. The delivered `y` already carries realistic weekly amplitude;
no further processing needed on the package side.
