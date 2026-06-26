# Handoff to `mozilla/mozaic-forecasting` — training-exclusion ("gap holiday") for internet shutdowns

**From:** Brendan Wells (Firefox DAU forecasting) · **Date:** 2026-06-26
**Context repo:** `mozaic-daily` branch `july-forecast` · **Related:** `~/work/holiday-corrected/FUTURE_WORK.md` (Data-coverage section documents the same Iran gap)

## The ask

Add a **training-exclusion mechanism** to mozaic that masks a *contiguous, multi-week-to-multi-month* date range out of the historical series before detrending + Prophet fitting, so the model **interpolates across the gap** instead of fitting trend/seasonality/changepoints to it.

Concrete trigger: **Iran's internet shutdown (~2026-02-28 → recovery in mid-2026).** Iran DAU collapsed from ~700k to ~1–40k for several months, then recovered. We are returning IR to native queries for the July forecast. If we feed the raw series (cliff down → flat-near-zero → cliff back up) into mozaic, Prophet places spurious changepoints and the world-level reconciliation is corrupted. We want to tell mozaic "ignore this window — it is a known artifact, not signal."

## Why the existing mechanisms don't cover this

I reviewed the package (`holiday_smart.py`, `tile.py`, `core.py`, `utils.py`, `models.py`). There are three adjacent features, none of which fits:

1. **`detrend()` holiday smoothing** (`holiday_smart.py:493`) — kinematic, radius-bounded (`max_radius` ≤ ~5 days). It smooths *short dips around recurring holidays*. A months-long gap is orders of magnitude wider than the radius; it won't be touched.
2. **`IranHolidays` blackout ranges + the "blackout"→`-1.0` rule** (`core.py:313-322`) — this *forces the forecast to ~zero* on blackout dates. That's for **predicting** a known-future outage, the opposite of what we need. We want the historical gap *ignored* so recovered data drives the forecast upward, not pinned to zero.
3. **`DesktopBugs` one-time events** (e.g. the Oct 2025 "Legacy Telemetry Drop") — closest in spirit ("interpolate over a known artifact"), but it's a fixed enumerated calendar of single-incident markers, not a general range-exclusion primitive, and it flows through the same radius-bounded detrend path.

So a months-long mid-series gap is genuinely new capability.

⚠️ **Likely interaction with the warmup-clamp bug you just fixed** (SHA `e97413b9`, "leading-near-zero warmup lock-in"). That fix trims *leading* near-zero rows. A shutdown gap is the same pathology **mid-series**: during the near-zero stretch the kinematic clamp's rolling reference statistics collapse toward 0 and can lock in (positive-feedback). Whatever exclusion mechanism you add should also prevent the gap from poisoning the clamp's reference stats — i.e. the masked rows must be removed from the kinematic loop's history, not just NaN'd at the end.

## Proposed design (least-invasive)

Thread an optional `excluded_date_ranges` parameter through the call chain, defaulting to `None` so existing behavior is byte-identical:

```
ModelConfig.excluded_date_ranges        # Optional[List[Tuple[str, str]]] = None
  → populate_tiles(..., excluded_date_ranges=None)
    → Tile(excluded_date_ranges=...)
      → Tile._detrend_holidays()
        → detrend(..., excluded_date_ranges=...)
```

In `detrend()` (`holiday_smart.py:493`), before the kinematic loop:

```python
if excluded_date_ranges:
    for start_str, end_str in excluded_date_ranges:
        mask = (df["submission_date"] >= pd.to_datetime(start_str)) & \
               (df["submission_date"] <= pd.to_datetime(end_str))
        df.loc[mask, "y"] = np.nan        # drop from training; Prophet sees a gap
    # IMPORTANT: exclude these rows from the rolling reference stats the
    # kinematic clamp uses, so the gap can't lock x_bar/v_bar/a_bar to ~0
    # (same failure class as the leading-warmup bug fixed in e97413b9).
```

Prophet already tolerates NaN `y` (mozaic converts detrended zeros back to NaN before `m.fit()` — `tile.py:67`), so a masked gap becomes ordinary missing data that Prophet interpolates across via trend + seasonality.

### Acceptance criteria
- [ ] `excluded_date_ranges=None` → outputs identical to current (regression-locked).
- [ ] With a synthetic series containing a 90-day mid-series near-zero gap + a recovery: with the range excluded, Prophet places **no changepoint inside the gap**, and the post-gap forecast tracks the recovered level (not the depressed gap level).
- [ ] Masked rows do **not** corrupt the kinematic clamp reference stats (guard against the warmup-class lock-in).
- [ ] Works at country *and* reconciled/aggregate level (the World mozaic should inherit the exclusion since the masked country contributes NaN, not zero, into the sum).
- [ ] Optional: a documented convenience — extend `IranHolidays` (or a new incident registry) so the Iran 2026 shutdown range is shipped as a named default, but the general `excluded_date_ranges` param is the load-bearing feature.

## What we need on our side (not blocking the PR)
- Exact gap window from BQ: IR daily DAU desktop + mobile, find shutdown start (~2026-02-28) and the recovery date where DAU returns to a stable level. We'll pass that as `excluded_date_ranges=[("2026-02-28", "<recovery>")]`.

## Pointers for the implementer
| Component | File:line | Role |
|---|---|---|
| Detrend (insertion point) | `holiday_smart.py:493-598` | add `excluded_date_ranges`; mask + protect clamp stats |
| Tile construction | `tile.py:11-129` | new field + pass-through to `detrend()` |
| Pipeline params | `utils.py:34-92` (`populate_tiles`) | new kwarg |
| Config | `models.py:12-40` (`ModelConfig`) | new field |
| Blackout/-1.0 rule (do NOT reuse) | `core.py:313-322` | shows why "blackout holiday" is the wrong tool here |
| Warmup fix to mirror | mozaic SHA `e97413b9` | same clamp-lock-in pathology, mid-series |
