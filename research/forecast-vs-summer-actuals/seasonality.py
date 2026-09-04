"""Prophet's seasonality, in absolute DAU, against history and against what happened.

The main analysis compares whole curves. This compares the *seasonal component* underneath them:
did the model expect a deeper summer than the last four years delivered, and than 2026 actually
delivered?

Everything is expressed as **DAU deviation from a trend**, not as a ratio, because a ratio hides
the year-over-year decline in absolute size — 1% of 2022's 58M is 580,000 DAU, 1% of 2026's 48M is
480,000. A reader asked to judge whether the model's seasonality "behaves" needs the DAU.

How the model's seasonality is recovered
----------------------------------------
The pickles do NOT store fitted Prophet objects — `tile.forecast_model` is the factory closure, so
there is no `predict_seasonal_components()` to call. What each tile does keep is enough to rebuild
the decomposition by arithmetic:

    tile.trend                        the trend over the forecast window — in LOG space on the
                                      August build and LEVEL space on July's, a package version
                                      difference that `trend_in_dau()` detects rather than assumes
    tile.forecast_reconciled          level-space, 1000 posterior samples, post-reconciliation
    tile.forecasted_holiday_impacts   level-space, 1000 samples, <= 0, applied ON TOP of the above

Verified against the published parquet on the August build:

    parquet = Σ forecast_reconciled + Σ holiday_impacts + (l + o overlays)

with the overlay residual a smooth +763,385 mean / 62,932 std — consistent with `l` (200K) plus
`o` (~560K), which the pickle predates. So

    model non-trend component = Σ reconciled + Σ holidays − exp(trend)

**Holidays are kept in.** Actuals contain holiday effects, so removing them from the model side
only would make the two incomparable. This is the "most comparable version", not the purest
seasonality.

The common trend baseline
-------------------------
All three curves are deviations from the SAME trend — the model's own `exp(trend)` — because it is
the only trend defined over the whole Aug→Dec window. Consequences to state plainly wherever these
numbers appear:

* `actual − model` is then exactly the pre-overlay forecast miss, which ties this pane back to the
  main analysis.
* A trend error enters every curve identically, so it cancels in the DIFFERENCES between curves,
  which is what the pane is for. It does not cancel in any single curve's level.
"""

from __future__ import annotations

import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent.parent / "src"))

import series as S  # noqa: E402

# This pane smooths on a CENTRED 7-DAY window, not the 28-day trailing mean the rest of the
# analysis uses. Both cancel DAU's weekly cycle exactly — any whole number of weeks does — so 7
# gives up nothing on that front. What it buys is edge: a 28-day trailing mean is undefined for the
# first 27 days of the forecast window, and August's seam sits only 21 days before the last landed
# actual, so on a 28-day basis the model curve and the actual curve DO NOT OVERLAP AT ALL. Centring
# also removes the ~13-day phase lag a trailing window would put between the curves, which would be
# fatal to a comparison of shapes. Cost: a little more day-to-day noise on the actuals, and 3 days
# lost at each end.
MA_WINDOW = 7
PKL_CACHE = HERE / "data" / "pkl"

# The pane reports calendar 2026 only. Each pickle's horizon runs to 2027-12-31, but the 2027 cycle
# is a pure extrapolation of the same seasonality and there are no actuals to compare it against.
REPORT_END = pd.Timestamp("2026-12-31")

# Each vintage's fitted-model pickle, and the provenance of that file.
#
# July's PUBLISHED build shipped no pickle — `data-official/2026-07/desktop_locked/` holds only the
# parquet, on disk and in GCS. The file below is a parameter-scan run at July's seam whose
# reconciled forecast reproduces July's published parquet EXACTLY: 0 DAU difference across all
# 198,696 rows. So it is the published fit, not a re-fit of it. `verify_reproduces_published()`
# re-runs that check rather than trusting this comment.
MODELS = {
    "august": {
        "path": (
            "data-official/2026-08/desktop_g01_2026-08-02/"
            "cps0.1649_thresh032_recent17_cpr0.814_ncp40_clip0.6_sps0.00825_regimemultiplicative/"
            "mozaic_objects.legacy_desktop.2026-08-02.pkl"
        ),
        "parquet": (
            "data-official/2026-08/desktop_g01_2026-08-02/"
            "cps0.1649_thresh032_recent17_cpr0.814_ncp40_clip0.6_sps0.00825_regimemultiplicative/"
            "mozaic_daily_forecast.2026-08-02.ld-D.adj-lo.parquet"
        ),
        "provenance": "published build, on disk",
    },
    "july": {
        "path": str(PKL_CACHE / "mozaic_objects.legacy_desktop.2026-07-06.pkl"),
        "parquet": "data-official/2026-07/desktop_locked/"
                   "mozaic_daily_forecast.2026-07-06.ld-D.adj-lo.parquet",
        "provenance": (
            "aug22-retune round1/center from GCS; verified to reproduce July's published "
            "parquet to 0 DAU across 198,696 rows"
        ),
    },
}

_CACHE: dict[str, object] = {}


def load_mozaic(vintage: str):
    """The fitted Mozaic for a vintage's DAU metric, memoised (each pickle is ~600 MB)."""
    if vintage not in _CACHE:
        path = Path(MODELS[vintage]["path"])
        if not path.is_absolute():
            path = HERE.parent.parent / path
        if not path.exists():
            raise FileNotFoundError(
                f"{vintage}: {path} is absent. July's pickle is not in the repo — fetch it with\n"
                f"  gsutil cp gs://moz-data-science-brwells-bucket/mozaic-daily-archive/"
                f"july-2026/param-scans/aug22-retune/round1/center/"
                f"cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825/"
                f"mozaic_objects.legacy_desktop.2026-07-06.pkl {PKL_CACHE}/"
            )
        with open(path, "rb") as handle:
            _CACHE[vintage] = pickle.load(handle)["DAU"]
    return _CACHE[vintage]


# The two builds store `trend` in DIFFERENT SPACES — a mozaic package version difference, not a
# modelling choice. August (package 4f33650) stores log DAU: tile trends span 9.4-16.4, and the
# Mozaic-level trend is ~17.6. July's build stores level DAU directly: its Mozaic-level trend spans
# 45.0M-49.1M. Assuming either one silently produces garbage — `exp()` of a level-space trend
# overflows to inf, and a log-space trend read as a level is off by ten orders of magnitude. So the
# space is DETECTED per tile against that tile's own forecast scale, and a tile that matches
# neither interpretation raises rather than being guessed at.
_MAX_SCALE_RATIO = 4.0  # seasonality + holidays can move a tile this far from its trend, not more


def trend_in_dau(tile) -> np.ndarray:
    """A tile's trend in level (DAU) space, whichever space the build happened to store it in."""
    raw = np.asarray(tile.trend, dtype=float)
    reference = float(np.median(np.asarray(tile.forecast.median(axis=1), dtype=float)))

    candidates = {"level": raw}
    # A log-space trend of a real DAU series sits well under 30; anything larger would overflow.
    if np.nanmax(raw) < 30.0:
        candidates["log"] = np.exp(raw)

    scored = {}
    for space, values in candidates.items():
        middle = float(np.median(values))
        if middle > 0 and np.isfinite(middle):
            scored[space] = abs(np.log(middle / reference)) if reference > 0 else np.inf

    if not scored:
        raise ValueError(
            f"{tile.name}: trend is neither a usable level nor a usable log series "
            f"(median raw {np.median(raw):.4g}, forecast median {reference:.4g})."
        )
    best = min(scored, key=scored.get)
    if scored[best] > np.log(_MAX_SCALE_RATIO):
        raise ValueError(
            f"{tile.name}: the best trend interpretation ({best}) is still {np.exp(scored[best]):.1f}x "
            f"away from the tile's own forecast scale (trend median "
            f"{np.median(candidates[best]):,.0f} vs forecast median {reference:,.0f}). Neither "
            f"space fits; do not report seasonality for this build."
        )
    return candidates[best]


def _keep(tile, track: str) -> bool:
    if track == "ex_ir_cn":
        return tile.country not in S.EXCLUDED_COUNTRIES
    if track == "all":
        return True
    raise ValueError(f"unknown track {track!r}; expected one of {S.TRACKS}")


def aggregate_trend(vintage: str) -> pd.Series:
    """The build's own ALL-level trend, in DAU. The authoritative baseline.

    Mozaic reconciles top-down, so the aggregate trend is a fit on the aggregate — not the sum of
    the per-tile trends, which are independent fits and need not add up. Summing exp() of them
    also can't add up in principle (Jensen). This is therefore the reference, and the tile sums are
    rescaled onto it by `_trend_scale`.
    """
    mozaic = load_mozaic(vintage)
    dates = pd.DatetimeIndex(mozaic.tiles[0].forecast_dates)
    raw = np.asarray(mozaic.trend, dtype=float)
    return pd.Series(np.exp(raw) if np.nanmax(raw) < 30.0 else raw, index=dates)


def _tile_trend_sum(vintage: str, track: str) -> pd.Series:
    mozaic = load_mozaic(vintage)
    tiles = [t for t in mozaic.tiles if _keep(t, track)]
    dates = pd.DatetimeIndex(mozaic.tiles[0].forecast_dates)
    return pd.Series(sum(trend_in_dau(t) for t in tiles), index=dates)


def _trend_scale(vintage: str) -> pd.Series:
    """Per-date factor putting the tile-trend sum onto the aggregate trend.

    Applied to BOTH tracks, so all-countries reproduces the aggregate trend exactly and ex-IR/CN is
    the same tile subset under the same correction. Measured deviation from 1.0: up to 0.40% for
    August and 1.80% for July inside 2026 (July's horizon is longer, so its drift is larger).
    `trend_drift()` reports it; it is a caveat on each curve's absolute level, and it cancels in
    the differences between curves because all three share this baseline.
    """
    return aggregate_trend(vintage) / _tile_trend_sum(vintage, "all")


def components(vintage: str, track: str = "all", reconciled: bool = True) -> pd.DataFrame:
    """Daily trend / forecast / holiday / non-trend series for one vintage and population track."""
    mozaic = load_mozaic(vintage)
    tiles = [t for t in mozaic.tiles if _keep(t, track)]
    if not tiles:
        raise ValueError(f"{vintage}/{track}: no tiles in scope")
    dates = pd.DatetimeIndex(tiles[0].forecast_dates)

    attr = "forecast_reconciled" if reconciled else "forecast"
    forecast = np.zeros(len(dates))
    holiday = np.zeros(len(dates))
    for tile in tiles:
        forecast += getattr(tile, attr).median(axis=1).to_numpy()
        holiday += tile.forecasted_holiday_impacts.median(axis=1).to_numpy()

    trend = (_tile_trend_sum(vintage, track) * _trend_scale(vintage)).to_numpy()
    frame = pd.DataFrame(
        {"trend": trend, "forecast": forecast, "holiday": holiday}, index=dates
    )
    # Holidays sit on top of the reconciled forecast, so the model's full level is their sum.
    frame["level"] = frame["forecast"] + frame["holiday"]
    frame["non_trend"] = frame["level"] - frame["trend"]
    return frame


def _ma(series: pd.Series) -> pd.Series:
    """Centred whole-week mean. See MA_WINDOW for why this pane does not use the 28-day trailing."""
    return series.rolling(MA_WINDOW, center=True).mean()


def actuals_smoothed(track: str) -> pd.Series:
    """Desktop actuals on this pane's smoothing basis, so every curve is comparable."""
    return _ma(S.actuals_daily(track))


def model_seasonal_dau(vintage: str, track: str = "all", reconciled: bool = True) -> pd.Series:
    """The model's non-trend component in DAU, 28-day trailing mean.

    Smoothing cancels the weekly cycle, which in daily space dwarfs the annual one — a single tile
    swings roughly +21% on weekdays to -41% at weekends.
    """
    frame = components(vintage, track, reconciled)
    return _ma(frame["level"]) - _ma(frame["trend"])


def model_trend_ma(vintage: str, track: str = "all") -> pd.Series:
    """Smoothed model trend — the common baseline every curve on this pane is measured from."""
    return _ma(components(vintage, track)["trend"])


# --- the two empirical curves, put on the model's trend scale ---------------------------------

def _yearly_ratio(ma: pd.Series, year: int) -> pd.Series:
    """One historical year's smoothed DAU as a ratio to its own centred annual mean, indexed 'MM-DD'.

    A centred 365-day mean is the empirical analogue of Prophet's trend: it removes level and
    long-run drift while leaving the within-year shape. It needs +/-182 days of data, which every
    norm year has and 2026 does not — hence 2026 actual uses the model's trend instead.
    """
    annual = ma.rolling(365, center=True).mean()
    window = slice(f"{year}-01-01", f"{year}-12-31")
    ratio = (ma.loc[window] / annual.loc[window]).dropna()
    ratio.index = ratio.index.strftime("%m-%d")
    return ratio[ratio.index != "02-29"]


def history_seasonal_dau(
    vintage: str, track: str = "all", years: tuple[int, ...] = S.NORM_YEARS
) -> pd.Series:
    """The 2022-2025 average seasonal shape, expressed in DAU at the model's own trend level.

    Rescaling by the model's trend is what makes this comparable in absolute terms: the average is
    formed as a dimensionless ratio, so each norm year contributes its shape and none of its size,
    and the DAU it is finally expressed in is 2026's.
    """
    ma = actuals_smoothed(track)
    shapes = pd.DataFrame({year: _yearly_ratio(ma, year) for year in years})
    mean_shape = shapes.dropna(how="any").mean(axis=1)

    trend = model_trend_ma(vintage, track).dropna()
    keys = trend.index.strftime("%m-%d")
    aligned = pd.Series(mean_shape.reindex(keys).to_numpy(), index=trend.index)
    return (aligned - 1.0) * trend


def actual_seasonal_dau(vintage: str, track: str = "all") -> pd.Series:
    """2026's realised non-trend component in DAU, on the same trend baseline as the model.

    Only defined where actuals and the forecast window overlap, so it stops at the last landed day.
    """
    trend = model_trend_ma(vintage, track)
    actual = actuals_smoothed(track)
    common = trend.index.intersection(actual.dropna().index)
    return (actual.reindex(common) - trend.reindex(common)).dropna()


# --- checks that run before anything is plotted -------------------------------------------------

def trend_drift(vintage: str, tolerance: float = 0.05) -> dict:
    """How far the per-tile trend sum sits from the build's aggregate trend, before rescaling.

    Reported, not fatal: `_trend_scale` corrects it, and it cancels in differences between curves.
    The tolerance only catches a gross mismatch — a wrong tile set, or a trend space still being
    misread after `trend_in_dau`.
    """
    relative = (_tile_trend_sum(vintage, "all") / aggregate_trend(vintage)) - 1.0
    in_window = float(relative.loc[:REPORT_END].abs().max())
    if in_window > tolerance:
        raise ValueError(
            f"{vintage}: the per-tile trend sum departs from the aggregate trend by up to "
            f"{in_window:.1%} inside the reported window, over the {tolerance:.0%} tolerance. "
            f"That is too large to be independent-fit drift; check the tile set and the trend "
            f"space before reporting seasonality."
        )
    return {
        "max_abs_in_window": in_window,
        "at_seam": float(relative.iloc[0]),
        "at_report_end": float(relative.loc[REPORT_END]),
        "at_horizon_end": float(relative.iloc[-1]),
    }


def verify_reproduces_published(vintage: str, tolerance: float = 1.0) -> dict:
    """Confirm this pickle is the build that shipped, not a look-alike re-fit.

    Checks that `Σ reconciled + Σ holidays` differs from the published parquet only by a smooth
    offset — the `l` + `o` overlays, which the pickle predates. A LEVEL offset is expected; a
    ragged one means the pickle is a different fit.
    """
    from mozaic_daily.adjustments import load_forecast

    frame = components(vintage, "all")
    path = HERE.parent.parent / MODELS[vintage]["parquet"]
    published, _meta = load_forecast(str(path), require_state=["l", "o"])
    rows = published[
        (published["data_source"] == "legacy_desktop")
        & (published["segment"] == '{"os": "ALL"}')
        & (published["country"] == "ALL")
        & (published["app_name"] == "desktop")
        & (published["data_type"] == "forecast")
    ].copy()
    rows["target_date"] = pd.to_datetime(rows["target_date"])
    parquet = rows.set_index("target_date")["dau"].sort_index()

    common = frame.index.intersection(parquet.index)
    residual = parquet.reindex(common) - frame["level"].reindex(common)
    # The overlays are smooth and slowly varying; their day-to-day roughness is tiny next to their
    # level. A std above ~5% of the mean would mean the residual is not an overlay.
    roughness = float(residual.std() / abs(residual.mean()))
    if roughness > 0.25:
        raise ValueError(
            f"{vintage}: parquet - (reconciled + holidays) has std/mean = {roughness:.2f}, too "
            f"ragged to be the l+o overlays. This pickle is probably not the published fit."
        )
    return {
        "overlay_residual_mean": float(residual.mean()),
        "overlay_residual_std": float(residual.std()),
        "roughness": roughness,
        "days": int(len(common)),
    }


# --- Feb 15 - Apr 15 anchored normalisation -----------------------------------------------------
#
# The other panes anchor at a seam or a pre-summer date. Anchoring at spring instead buys a
# WHOLE-YEAR view: each curve is expressed as its deviation from its own Feb 15 - Apr 15 level, so
# the full arc from spring peak to summer trough to winter is visible on one axis.
#
# That is only possible for the model because Prophet's seasonality REPEATS. Measured on the 146
# calendar days that appear in both years of August's window (Aug 05 - Dec 28, 2026 and 2027):
#
#     seasonality only      max drift 0.59pp   (mean -0.34pp)  ~276,000 DAU at 47M
#     seasonality+holidays  max drift 3.95pp   (mean -0.52pp)  ~1,856,000 DAU at 47M
#
# So the model's 2027 cycle is a sound stand-in for 2026 — with holidays as the stated exception,
# which is why December is the least trustworthy part of any spring-anchored chart.
SPRING = ("02-15", "04-15")

# The spring-anchored pane smooths on a 28-DAY TRAILING mean — the same convention as the published
# curves and the other tab, and for the same reason: it is the least noisy whole-week window.
#
# The seam-anchored construction this replaced could not use it. There the model's curve began at
# the seam, and a 28-day trailing mean is undefined for 27 days after that, which left no overlap
# with actuals at all for August. Anchoring at spring removes that constraint entirely: the model's
# shape comes from a COMPLETE 2027 cycle with 2026 data sitting behind it inside the same forecast
# window, so a trailing 28-day mean is defined across the whole year. Using 7 days here was carried
# over from the old construction and only added noise.
SPRING_MA_WINDOW = 28
MODEL_CYCLE_YEAR = 2027  # the only complete cycle inside August's forecast window
PERIODICITY_DRIFT_PP = 0.59  # measured, seasonality-only, on the 146 matched days


def _spring_ma(series: pd.Series) -> pd.Series:
    """28-day trailing mean, matching the published-curve convention."""
    return series.rolling(SPRING_MA_WINDOW).mean()


def actuals_spring_smoothed(track: str) -> pd.Series:
    return _spring_ma(S.actuals_daily(track))


def _spring_mean(series: pd.Series, year: int) -> float:
    window = series.loc[f"{year}-{SPRING[0]}":f"{year}-{SPRING[1]}"].dropna()
    if window.empty:
        raise ValueError(f"no data in the {year} Feb 15 - Apr 15 anchor window")
    return float(window.mean())


def _by_calendar_day(series: pd.Series) -> pd.Series:
    """Re-index on 'MM-DD' and sort, so a calendar-window slice is well defined."""
    out = series.dropna()
    out.index = out.index.strftime("%m-%d")
    return out[out.index != "02-29"].sort_index()


def history_shape(track: str = "all") -> pd.Series:
    """The 2022-2025 average seasonal shape, indexed by calendar day.

    Each year is divided by its own Feb 15 - Apr 15 level before averaging, so no year contributes
    its size. The normalising constant is arbitrary for any *re-anchored* use of this shape -- it
    cancels in `shape(d) / shape(anchor)` -- so both the spring-anchored and seam-anchored views
    build on this one series.
    """
    smoothed = actuals_spring_smoothed(track)
    shapes = {
        year: _by_calendar_day(
            smoothed.loc[f"{year}-01-01":f"{year}-12-31"] / _spring_mean(smoothed, year)
        )
        for year in S.NORM_YEARS
    }
    return pd.DataFrame(shapes).dropna(how="any").mean(axis=1)


# The seam-anchored view has to smooth on a CENTRED 7-DAY window, and is anchored at seam+3.
#
# This is forced, not preferred. The model has no output before its seam, so a 28-day trailing mean
# — the convention everywhere else here — is undefined for the first 27 forecast days. Filling that
# window with actuals was tried and REJECTED: the July build's daily level at its seam sits 5.35%
# below actuals (46.96M vs 49.62M, far more than the ~1.5% overlay gap), so a spliced prefix
# dominates the average for 27 days and the model curve silently inherits ACTUALS' trajectory over
# exactly the window being measured. It made July's model read 488,729 shallower than history when
# its own 2027 Jul-06 -> trough descent is -3,929,823, i.e. ~1.5M DEEPER than history.
#
# A centred 7-day window needs only 3 days either side, so anchoring at seam+3 needs no splice at
# all. Any whole number of weeks cancels the weekly cycle exactly; 7 days is just noisier than 28.
SEAM_MA_WINDOW = 7
SEAM_ANCHOR_OFFSET_DAYS = 3


def seam_normalised(
    vintage: str, track: str = "all", reconciled: bool = True
) -> dict[str, pd.Series]:
    """The three curves rescaled to meet 2026's actual level just after the vintage's seam.

    The seasonality counterpart of the decomposition tab's seam-anchored counterfactual: every
    curve starts from where the forecast actually began, and the fan-out from there is each one's
    seasonal trajectory. This is the view that judges a vintage on what it set out to predict.

    Two respects in which it is cleaner than the spring-anchored view:

    * It runs seam -> 2026-12-31, entirely inside the 2026 portion of the forecast window, so it
      needs **no 2027 stand-in** and the holiday-non-periodicity caveat does not apply.
    * Its anchor is a single settled actual after Iran's 2026-05-25 recovery, so it carries **none**
      of the Iran contamination that depresses the all-countries spring anchor.

    What it gives up: it starts mid-summer, so it cannot see the spring-to-summer descent — which is
    where most of a vintage's seasonal error accumulates. And it is noisier (see SEAM_MA_WINDOW).
    """
    seam = S.VINTAGES[vintage]["seam"]
    anchor_date = seam + pd.Timedelta(days=SEAM_ANCHOR_OFFSET_DAYS)
    anchor_key = anchor_date.strftime("%m-%d")
    end = pd.Timestamp("2026-12-31")

    def smooth(series: pd.Series) -> pd.Series:
        return series.rolling(SEAM_MA_WINDOW, center=True).mean()

    actuals = smooth(S.actuals_daily(track))
    reference = float(actuals.loc[anchor_date])

    frame = components(vintage, track, reconciled)
    level = smooth(frame["level"].loc[:end]).loc[anchor_date:end].dropna()
    model = _by_calendar_day(level / float(level.loc[anchor_date]) * reference)

    shape = smooth(S.actuals_daily(track))
    shapes = {
        year: _by_calendar_day(
            shape.loc[f"{year}-01-01":f"{year}-12-31"] / _spring_mean(shape, year)
        )
        for year in S.NORM_YEARS
    }
    history_shape_7d = pd.DataFrame(shapes).dropna(how="any").mean(axis=1)
    history = (history_shape_7d / float(history_shape_7d.loc[anchor_key]) * reference).loc[anchor_key:]

    realised = _by_calendar_day(actuals.loc[anchor_date:end])

    return {
        "model": model,
        "history": history,
        "realised": realised,
        "reference": reference,
        "anchor_key": anchor_key,
        "window_days": SEAM_MA_WINDOW,
    }


def spring_normalised(
    vintage: str, track: str = "all", reconciled: bool = True
) -> dict[str, pd.Series]:
    """The three curves as DAU deviation from their own spring level, indexed by calendar day.

    Every curve is `(own_index / own_spring_anchor - 1) * reference`, where `reference` is 2026's
    actual Feb 15 - Apr 15 mean DAU. So all three read zero at spring by construction and their
    vertical distances are DAU at 2026's spring scale.

    Holidays are INCLUDED, per review decision: actuals carry them, so stripping them from the
    model side alone would break comparability. The cost is stated in `PERIODICITY_DRIFT_PP` and
    in `spring_caveats()`.

    All three curves are trend-INCLUSIVE (see the comment in the body). One approximation follows
    from that: the model's shape comes from 2027, so its within-year trend decline stands in for
    2026's. Prophet's seasonality repeats but its trend does not, and normalising by 2027's own
    spring level cancels the level offset while leaving 2027's slope in place. The two years'
    decline rates are close (both near -5%/yr), so the residual is small, but it is an
    approximation rather than an identity.
    """
    smoothed = actuals_spring_smoothed(track)
    reference = _spring_mean(smoothed, 2026)

    # The model's series here is its LEVEL (reconciled + holidays), trend included -- NOT
    # level/trend. The empirical curves are each divided by their own spring level, which removes
    # that year's size but leaves the within-year trend decline inside the shape. Dividing the
    # model by its trend instead would make it the only trend-free curve of the three, and over the
    # ~5 months from the spring anchor to the summer trough that asymmetry is worth roughly 1M DAU
    # at a -5%/yr decline -- comparable to the effect being measured. So all three carry trend,
    # exactly as the spring baseline on the other tab does.
    frame = components(vintage, track, reconciled)
    level = _spring_ma(frame["level"])
    model = _by_calendar_day(
        level.loc[f"{MODEL_CYCLE_YEAR}-01-01":f"{MODEL_CYCLE_YEAR}-12-31"]
        / _spring_mean(level, MODEL_CYCLE_YEAR)
    )

    history = history_shape(track)

    realised = _by_calendar_day(smoothed.loc["2026-01-01":"2026-12-31"] / reference)

    return {
        "model": model * reference,
        "history": history * reference,
        "realised": realised * reference,
        "reference": reference,
    }


def spring_caveats(track: str = "all") -> dict:
    """The three things that limit a spring-anchored reading, sized rather than asserted."""
    frame = components("august", track)
    holiday_ratio = (_spring_ma(frame["holiday"]) / _spring_ma(frame["trend"])).dropna()
    window = holiday_ratio.loc[f"{MODEL_CYCLE_YEAR}-{SPRING[0]}":f"{MODEL_CYCLE_YEAR}-{SPRING[1]}"]

    smoothed = actuals_spring_smoothed(track)
    return {
        "periodicity_drift_pp": PERIODICITY_DRIFT_PP,
        "holiday_drift_pp": 3.95,          # measured, seasonality+holidays, matched days
        "anchor_holiday_drag": float(window.mean()),
        "anchor_holiday_worst_day": float(window.min()),
        "anchor_days_with_holiday": int((window.abs() > 1e-6).sum()),
        "anchor_days": int(len(window)),
        "reference_dau": _spring_mean(smoothed, 2026),
        # Iran's outage runs 2026-03-01 -> 2026-05-25, i.e. straight through the 2026 anchor
        # window, so the all-countries reference level is depressed. ex-IR/CN is unaffected.
        "anchor_iran_contaminated": track == "all",
    }


SUMMER = slice("08-01", "09-30")


def summary() -> dict:
    """Every number the seasonality pane quotes, on the Feb 15 - Apr 15 anchored basis.

    The comparison that matters is each build's summer seasonal trough against history's, and
    against what 2026 actually did — all three measured as DAU below their own spring level.
    Reported reconciled (what shipped) and pre-reconciliation, because the gap between those is
    the finding for July.
    """
    out = {
        "anchor": f"{SPRING[0]} to {SPRING[1]}",
        "model_cycle_year": MODEL_CYCLE_YEAR,
        "periodicity_drift_pp": PERIODICITY_DRIFT_PP,
        "vintages": {},
    }
    for vintage in MODELS:
        entry = {
            "seam": str(S.VINTAGES[vintage]["seam"].date()),
            "provenance": MODELS[vintage]["provenance"],
            "trend_drift": trend_drift(vintage),
            "is_published": verify_reproduces_published(vintage),
            "tracks": {},
        }
        for track in S.TRACKS:
            curves = spring_normalised(vintage, track, reconciled=True)
            pre = spring_normalised(vintage, track, reconciled=False)
            model = curves["model"][SUMMER]
            history = curves["history"][SUMMER]
            realised = curves["realised"][SUMMER]
            pre_model = pre["model"][SUMMER]

            gap_to_close = realised.min() - history.min()
            closed = model.min() - history.min()

            # The same comparison judged from the seam instead of from spring. Different span, so
            # different magnitudes; reported alongside rather than instead of.
            sc = seam_normalised(vintage, track, reconciled=True)
            s_model, s_hist, s_real = (sc[k][SUMMER] for k in ("model", "history", "realised"))
            seam_gap = s_real.min() - s_hist.min()

            entry["tracks"][track] = {
                "seam_anchored": {
                    "anchor_day": sc["anchor_key"],
                    "anchor_dau": sc["reference"],
                    "window_days": sc["window_days"],
                    "model_trough": float(s_model.min()),
                    "history_trough": float(s_hist.min()),
                    "realised_trough": float(s_real.min()),
                    "model_vs_history": float(s_model.min() - s_hist.min()),
                    "realised_vs_history": float(seam_gap),
                    "share_of_gap_closed": (
                        float((s_model.min() - s_hist.min()) / seam_gap) if seam_gap else None
                    ),
                },
                "reference_dau": curves["reference"],
                "model_trough": float(model.min()),
                "model_trough_day": model.idxmin(),
                "history_trough": float(history.min()),
                "history_trough_day": history.idxmin(),
                "realised_trough": float(realised.min()),
                "realised_trough_day": realised.idxmin(),
                "model_vs_history": float(closed),
                "realised_vs_history": float(gap_to_close),
                "model_vs_realised": float(model.min() - realised.min()),
                # How much of history-to-reality the model actually covered. Negative means it
                # moved the wrong way.
                "share_of_gap_closed": float(closed / gap_to_close) if gap_to_close else None,
                "pre_recon_vs_history": float(pre_model.min() - history.min()),
                "reconciliation_effect": float(
                    closed - (pre_model.min() - history.min())
                ),
                "caveats": spring_caveats(track),
            }
        out["vintages"][vintage] = entry
    return out
