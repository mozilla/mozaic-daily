"""The three series — A (published forecast), B (typical summer), C (actuals) — on one basis.

Everything here is desktop-only, legacy telemetry, 28-day trailing moving average, in absolute
DAU. Two population tracks are supported throughout:

    "all"       world total, all countries. Matches what the forecast forecasts, but is NOT
                year-comparable: Iran's 2026 outage depresses its own baseline and China's
                MozillaOnline migration adds mid-year level that is not organic growth.
    "ex_ir_cn"  Iran and China removed. The year-comparable track. Note that removing CN also
                removes ~93% of the `o` MozillaOnline overlay from the forecast and (by
                construction) the real migration from actuals, so the two sides stay consistent.

Series definitions
------------------
A  the published canonical curve for a vintage, exactly as shipped: display-layer `h` headwind
   applied, `display_ma` seam splice included. This is what stakeholders read.
B  "what 2026 would have done with an ordinary summer": each of 2022-2025 rebased to its own
   baseline, averaged by calendar day, then rescaled by 2026's actual baseline. B carries 2026's
   level from actuals and history's shape.
C  actuals, 28-day trailing mean of real telemetry.

B is not purely seasonal: each historical year's rebased shape also contains that year's own
trend over the same months, so B implicitly assumes 2026's summer trend equals the 2022-2025
average. `trend_table()` exists to make that assumption inspectable rather than hidden, and
`overlay_ma()` sizes the two 2026-only level events (`o`, `l`) that history does not contain.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))
sys.path.insert(0, str(REPO_ROOT / "src"))

from export_desktop_ex_ir_cn_csv import (  # noqa: E402
    forecast_ma,
    load_country_dau,
    scope_daily,
)
from fetch_actuals import load_actuals  # noqa: E402

MA_WINDOW = 28
EXCLUDED_COUNTRIES = ("IR", "CN")
TRACKS = ("all", "ex_ir_cn")
NORM_YEARS = (2022, 2023, 2024, 2025)

# The evaluation window. Starts at August's seam (the vintage under test) and ends at the last
# day BigQuery has landed, which is CURRENT_DATE(LA) - 2 in the canonical actuals query.
EVAL_START = pd.Timestamp("2026-08-02")
EVAL_END = pd.Timestamp("2026-08-23")

# Each canonical vintage: its seam, its published CSV, and the column holding its forecast.
# `parquet`/`adjustments` are present only where an ex-IR/CN curve has to be rebuilt from the
# daily series (the published CSVs carry world totals only).
VINTAGES = {
    "august": {
        "seam": pd.Timestamp("2026-08-02"),
        "csv": "data-official/2026-08/csv/august_canonical_curves.csv",
        "column": "desktop_current_august",
        "csv_ex": "data-official/2026-08/csv/august_canonical_curves.DESKTOP_ONLY.EX_IR_CN.csv",
        "column_ex": "desktop_current_august_EX_IR_CN",
        "adjustments": "data-official/2026-08/adjustments",
        "csv_nh": ("data-official/2026-08/csv/"
                   "august_canonical_curves.DESKTOP_ONLY.WIN10_HEADWIND_REMOVED.csv"),
        "column_nh": "desktop_current_august_NO_WIN10_HEADWIND",
        "csv_nh_ex": ("data-official/2026-08/csv/"
                      "august_canonical_curves.DESKTOP_ONLY.WIN10_HEADWIND_REMOVED.EX_IR_CN.csv"),
        "column_nh_ex": "desktop_current_august_NO_WIN10_HEADWIND_EX_IR_CN",
    },
    "july": {
        "seam": pd.Timestamp("2026-07-06"),
        "csv": "data-official/2026-07/csv/july_canonical_curves.csv",
        "column": "desktop_current_july",
        "parquet": (
            "data-official/2026-07/desktop_locked/"
            "mozaic_daily_forecast.2026-07-06.ld-D.adj-lo.parquet"
        ),
        "adjustments": "data-official/2026-07/adjustments",
    },
    "june": {
        "seam": pd.Timestamp("2026-05-26"),
        "csv": "data-official/2026-06/csv/june_canonical_curves.csv",
        "column": "desktop_current_june_plus_iran",
        "adjustments": "data-official/2026-06/adjustments",
    },
}

# Desktop adjustments on a published curve, and whether this analysis can remove one.
#
#   `h`  Win10 headwind   display-layer linear ramp applied to the 28d MA after mozaic, so
#                         `published - ramp` is EXACT and needs no model re-run.
#   `t`  mobile tailwind  desktop_dau is 0 in the spec — it contributes exactly nothing here.
#   `l`  launch-on-login  per-tile BIDIRECTIONAL overlay: subtracted from training before mozaic,
#   `o`  MozillaOnline    added back after. Removing either means re-running the model, and past
#                         builds are locked. Their add-back level is an indicative magnitude, NOT
#                         a counterfactual — Prophet's fit changes when the subtraction changes,
#                         so the realised effect is config-dependent and is not a level shift.
REMOVABLE_ADJUSTMENTS = ("h",)
BAKED_IN_ADJUSTMENTS = ("l", "o")


# --- C: actuals -------------------------------------------------------------------------------

def actuals_daily(track: str = "all") -> pd.Series:
    """Daily desktop DAU actuals for a population track, date-indexed."""
    frame = load_actuals()
    if track == "ex_ir_cn":
        frame = frame[~frame["country"].isin(EXCLUDED_COUNTRIES)]
    elif track != "all":
        raise ValueError(f"unknown track {track!r}; expected one of {TRACKS}")
    daily = frame.groupby("date")["dau"].sum().sort_index()
    daily.index = pd.DatetimeIndex(daily.index)
    return daily


def ma28(daily: pd.Series) -> pd.Series:
    """Plain 28-day trailing mean. Used for actuals and for every historical year."""
    return daily.sort_index().rolling(MA_WINDOW).mean()


def actuals_ma(track: str = "all") -> pd.Series:
    """Series C — the 28-day trailing mean of real telemetry."""
    return ma28(actuals_daily(track))


# --- A: the published forecast ----------------------------------------------------------------

def published_forecast(vintage: str, track: str = "all") -> pd.Series:
    """Series A — a vintage's published curve, `h` applied and seam splice included.

    The all-countries track reads the shipped CSV directly, so it is the published number by
    construction. The ex-IR/CN track reads the shipped CSV where one exists (August) and
    otherwise rebuilds from the parquet, differencing the DAILY series before re-running
    `display_ma` — subtracting per-country MAs is invalid because the splice is non-linear.
    """
    spec = VINTAGES[vintage]

    if track == "all":
        column, path = spec["column"], spec["csv"]
    elif track == "ex_ir_cn":
        if "csv_ex" in spec:
            column, path = spec["column_ex"], spec["csv_ex"]
        else:
            return _rebuild_ex_ir_cn_forecast(vintage)
    else:
        raise ValueError(f"unknown track {track!r}; expected one of {TRACKS}")

    frame = pd.read_csv(REPO_ROOT / path, parse_dates=["date"]).set_index("date")
    return frame[column].dropna()


def desktop_headwind_ramp(vintage: str, index: pd.DatetimeIndex) -> pd.Series:
    """A vintage's `h` ramp over `index`: 0 at its seam, its full anchor at 2026-12-15.

    Negative by convention — it is a headwind. Read from that cycle's own spec, so each vintage
    carries its own anchor AND its own ramp-start convention (July's starts 2026-04-01, August's
    at the seam), which is a large part of why their summer-window drags differ.
    """
    from export_desktop_no_headwind_csv import load_desktop_headwind_ramp

    spec = VINTAGES[vintage]
    return load_desktop_headwind_ramp(
        str(REPO_ROOT / spec["adjustments"]), index, spec["seam"]
    )


def published_forecast_no_headwind(vintage: str, track: str = "all") -> pd.Series:
    """Series A with `h` removed — the model's own curve, `l` and `o` still baked in.

    August ships this as a published counterfactual CSV, so that file is used directly and the
    arithmetic identity `A_nh == A - ramp` is asserted against it rather than assumed. Other
    vintages have no such file, so the ramp is subtracted from the published curve; because `h`
    is display-layer that subtraction is exact.
    """
    spec = VINTAGES[vintage]
    published = published_forecast(vintage, track)
    ramp = desktop_headwind_ramp(vintage, published.index)
    derived = published - ramp

    key = "csv_nh_ex" if track == "ex_ir_cn" else "csv_nh"
    col = "column_nh_ex" if track == "ex_ir_cn" else "column_nh"
    if key not in spec:
        return derived

    frame = pd.read_csv(REPO_ROOT / spec[key], parse_dates=["date"]).set_index("date")
    shipped = frame[spec[col]].dropna()
    # The shipped file is rounded to published integers; the derived series is not. A tolerance of
    # 1 DAU catches a genuine convention mismatch while allowing that rounding.
    gap = (shipped - derived.reindex(shipped.index)).abs().max()
    if gap > 1.0:
        raise ValueError(
            f"{vintage}/{track}: the shipped NO_WIN10_HEADWIND curve and `published - ramp` "
            f"disagree by up to {gap:,.2f} DAU. One of them is not the display-layer `h` this "
            f"analysis assumes; do not report a headwind component until that is resolved."
        )
    return shipped


def _rebuild_ex_ir_cn_forecast(vintage: str) -> pd.Series:
    """Ex-IR/CN published curve for a vintage whose shipped CSVs carry world totals only."""
    spec = VINTAGES[vintage]
    if "parquet" not in spec:
        raise NotImplementedError(
            f"{vintage}: no ex-IR/CN CSV and no parquet on disk, so the ex-IR/CN curve cannot be "
            f"rebuilt. Use track='all' for this vintage, or archive-restore its parquet."
        )
    from export_desktop_no_headwind_csv import load_desktop_headwind_ramp

    pivot, _training = load_country_dau(str(REPO_ROOT / spec["parquet"]))
    curve = forecast_ma(scope_daily(pivot), spec["seam"])
    ramp = load_desktop_headwind_ramp(
        str(REPO_ROOT / spec["adjustments"]), curve.index, spec["seam"]
    )
    return (curve + ramp).loc[spec["seam"]:].dropna()


# --- B: the typical-summer counterfactual -------------------------------------------------------

BASELINES = {
    # Pre-summer and clear of Iran's 2026-03-01 -> 2026-05-25 outage, so 2026's anchor is settled
    # real telemetry. This is the same baseline `research/autumn-decoupling` uses, for the same
    # reason.
    "jun15": ("06-15", "06-15"),
    # The handoff's choice. On the all-countries track 2026's window sits INSIDE the Iran outage,
    # which depresses the 2026 anchor and inflates C - B. Clean on the ex-IR/CN track.
    "spring": ("02-15", "04-15"),
    # Anchored at August's seam, so B(seam) == C(seam) by construction and every series starts
    # the evaluation window from the same point. This is the only baseline that decomposes the
    # AUGUST VINTAGE's own miss: August starts from actuals, so a pre-seam anchor would charge it
    # for June-July divergence it never forecast. The pre-summer anchors answer the different,
    # whole-season question of how much the summer as a whole beat a typical one.
    "seam": ("08-02", "08-02"),
}


def baseline_window(kind: str) -> tuple[str, str]:
    """The (start, end) MM-DD pair for a baseline, accepting either a named kind or a raw pair."""
    if isinstance(kind, tuple):
        return kind
    if kind not in BASELINES:
        raise ValueError(f"unknown baseline {kind!r}; expected one of {tuple(BASELINES)} or an "
                         f"explicit ('MM-DD', 'MM-DD') pair")
    return BASELINES[kind]


def baseline_value(ma: pd.Series, year: int, kind: str = "jun15") -> float:
    """One year's anchor level: the mean of its 28-day MA over the baseline window."""
    start_mmdd, end_mmdd = baseline_window(kind)
    window = ma.loc[f"{year}-{start_mmdd}":f"{year}-{end_mmdd}"].dropna()
    if window.empty:
        raise ValueError(f"{year}: no MA data in the {kind} baseline window")
    return float(window.mean())


def _rebased_shape(ma: pd.Series, year: int, kind: str) -> pd.Series:
    """One year's 28d-MA expressed as a fraction of its own baseline, indexed by 'MM-DD'."""
    year_ma = ma.loc[f"{year}-01-01":f"{year}-12-31"].dropna()
    shape = year_ma / baseline_value(ma, year, kind)
    shape.index = year_ma.index.strftime("%m-%d")
    # Feb 29 exists in one norm year only; averaging it across four would weight it wrong.
    return shape[shape.index != "02-29"]


def typical_summer(
    track: str = "all", baseline: str = "jun15", years: tuple[int, ...] = NORM_YEARS
) -> pd.Series:
    """Series B — the average 2022-2025 seasonal shape, rescaled to 2026's own baseline."""
    ma = actuals_ma(track)
    shapes = pd.DataFrame({year: _rebased_shape(ma, year, baseline) for year in years})
    # Require every norm year to contribute, so B is never silently an average of three.
    mean_shape = shapes.dropna(how="any").mean(axis=1)

    anchor = baseline_value(ma, 2026, baseline)
    dates = pd.to_datetime("2026-" + mean_shape.index)
    return pd.Series((mean_shape * anchor).to_numpy(), index=dates).sort_index()


# --- The 2026-only level events history does not contain ----------------------------------------

OVERLAYS = {
    "o": {
        "label": "MozillaOnline migration",
        "spec": "data-official/2026-08/mozillaonline/mozillaonline.json",
        "dir": "data-official/2026-08/mozillaonline",
    },
    "l": {
        "label": "launch-on-login",
        "spec": "data-official/2026-08/launch_on_login/lol.json",
        "dir": "data-official/2026-08/launch_on_login",
    },
}


def overlay_delta(code: str, track: str, baseline: str) -> float:
    """How much an overlay GREW between the baseline anchor and the end of the eval window.

    This, not the overlay's level, is what contaminates a baseline-anchored `C - B`. B is rescaled
    so it matches 2026 at the anchor, which means anything the overlay had already contributed by
    then is inside both series and cancels. Only the subsequent growth shows up as apparent
    "shallow summer".
    """
    curve = overlay_ma(code, track)
    start_mmdd, end_mmdd = baseline_window(baseline)
    anchor = curve.loc[f"2026-{start_mmdd}":f"2026-{end_mmdd}"].dropna().mean()
    return float(curve.reindex([EVAL_END]).iloc[0] - anchor)


def overlay_ma(code: str, track: str = "all") -> pd.Series:
    """28-day MA of an overlay's daily curve, scaled to the population track.

    `o` is allocated by fixed geo shares, so its ex-IR/CN portion is exactly the non-CN share
    (IR is already excluded from the spec's scope, then shares are renormalized). `l` is
    allocated by trailing DAU share, so its IR/CN slice leaves in proportion to those countries'
    share of desktop DAU — approximated here by their share over the evaluation window.
    """
    import json

    meta = OVERLAYS[code]
    spec = json.loads((REPO_ROOT / meta["spec"]).read_text())
    curve = pd.read_parquet(REPO_ROOT / meta["dir"] / spec["data_file"])
    # Both overlay curves carry the date on the INDEX (`target_date`), not as a column, and ship a
    # precomputed `_ma` alongside the daily values. The daily column is the one to read: the MA is
    # recomputed here so every series in this analysis shares one 28-day convention.
    daily = pd.Series(
        curve[spec["value_column"]].to_numpy(),
        index=pd.DatetimeIndex(pd.to_datetime(curve.index)),
    ).sort_index()

    if track == "ex_ir_cn":
        daily = daily * _overlay_ex_share(code, spec)
    return ma28(daily)


def _overlay_ex_share(code: str, spec: dict) -> float:
    """Fraction of an overlay that survives removing IR and CN."""
    if spec["allocation"]["key"] == "fixed_country_shares":
        shares = spec["allocation"]["shares"]
        # IR is already out of scope for `o`; the remaining shares are renormalized over the
        # countries present in training, so the surviving fraction is simply 1 - CN's share.
        return 1.0 - shares.get("CN", 0.0) / sum(shares.values())
    # trailing_dau_share: the overlay follows DAU, so IR+CN take their DAU share of it.
    daily_all = actuals_daily("all").loc[EVAL_START:EVAL_END].sum()
    daily_ex = actuals_daily("ex_ir_cn").loc[EVAL_START:EVAL_END].sum()
    return float(daily_ex / daily_all)


# --- The trend intuition check -------------------------------------------------------------------

def trend_table(track: str = "all", baseline: str = "jun15") -> pd.DataFrame:
    """Per-year seasonal profile, so the trend baked into B can be eyeballed rather than assumed.

    Each row is one year expressed against its OWN baseline, which is what makes the years
    comparable. The `aug23_ratio` column is the direct read on "was this summer shallow?" — it is
    the same calendar day on every row, so 2026 sitting above the norm years means 2026 had given
    up less of its pre-summer level by late August than a typical year does.
    """
    ma = actuals_ma(track)
    rows = []
    for year in (*NORM_YEARS, 2026):
        try:
            base = baseline_value(ma, year, baseline)
        except ValueError:
            continue
        window = ma.loc[f"{year}-06-15":f"{year}-09-30"].dropna()
        aug23 = ma.get(pd.Timestamp(f"{year}-08-23"), float("nan"))
        # 2026 is still mid-slump, so its "trough" is only the minimum seen SO FAR.
        complete = window.index.max() >= pd.Timestamp(f"{year}-09-30")
        rows.append(
            {
                "year": year,
                "baseline": base,
                "aug23": float(aug23),
                "aug23_ratio": float(aug23) / base,
                "trough": float(window.min()),
                "trough_date": window.idxmin().date().isoformat(),
                "trough_ratio": float(window.min()) / base,
                "season_complete": complete,
            }
        )
    table = pd.DataFrame(rows).set_index("year")
    table["baseline_yoy"] = table["baseline"].pct_change()
    return table
