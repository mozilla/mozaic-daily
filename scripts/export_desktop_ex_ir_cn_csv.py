#!/usr/bin/env python3
"""Export DESKTOP-ONLY, ex-Iran/ex-China (`EX_IR_CN`) twins of the cycle's canonical CSVs.

Four files, two scopes x two file kinds:

    august_canonical_curves.DESKTOP_ONLY.EX_IR_CN.csv                        # `h` applied
    august_dec15_summary.DESKTOP_ONLY.EX_IR_CN.csv                           # `h` applied
    august_canonical_curves.DESKTOP_ONLY.WIN10_HEADWIND_REMOVED.EX_IR_CN.csv # `h` removed
    august_dec15_summary.DESKTOP_ONLY.WIN10_HEADWIND_REMOVED.EX_IR_CN.csv    # `h` removed

Why this needs the parquets
---------------------------
Unlike `export_desktop_no_headwind_csv.py`, this cannot be derived from the published CSVs: they
carry only world (`country == "ALL"`) totals. It also cannot be done by subtracting per-country
28-day MAs, because `display_ma`'s variance-matched seam splice is **non-linear** — subtracting
MAs differs from the MA of the ex-IR/CN daily series by up to ~2,900 DAU inside the splice window.
So the daily series is differenced first, and `display_ma` is recomputed on the result.

That the difference is exact rests on `ALL == sum(named countries)` holding to 0 DAU in the
parquet, which `check_country_reconciliation` asserts rather than assumes.

Headwind treatment
------------------
The canonical scope applies each cycle's **full, unscaled** anchor (August -1,315,000, July
-1,345,000) to the ex-IR/CN curve. Two supports: the Win10 mechanism was measured ex-IR/CN in the
first place (`data-official/2026-08/adjustments/_index.md`), and `scripts/score_near_horizon.py`'s
`ex_cn_ir` scope already does exactly this. No geo re-allocation of `h` exists, and none is invented
here.

Two consequences of the scope worth knowing before quoting these files
----------------------------------------------------------------------
1. **Excluding CN removes ~93% of the `o` MozillaOnline tailwind.** It is baked into the parquet
   per-tile at fixed ~93%-CN geo shares, so dropping CN drops it. These curves are not "world minus
   5.8%" — they are also curves with almost no MozillaOnline migration in them.
2. **Excluding IR removes the counterfactual gap-fill from training and the shutdown crater from
   actuals**, so the ex-IR/CN series is entirely real telemetry. `l` (launch-on-login) is allocated
   by trailing-DAU share, so IR/CN's slice of it leaves automatically.

Actuals come from the parquet's `training` rows (verified real telemetry — IR's rows carry the
actual crater, not the fill), so they end one day earlier than the published files' BigQuery-sourced
column. That is asserted, not just described.

Usage:
    python scripts/export_desktop_ex_ir_cn_csv.py [--csv-dir DIR] [--dry-run]

Cycle-scoped: the constants below point at the August 2026 cycle. Repoint them at roll-forward.
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from mozaic_daily.adjustments import load_forecast  # noqa: E402
from mozaic_daily.seam_ma import daily_to_28ma, display_ma  # noqa: E402

# Reused rather than reimplemented: the repo already carries five diverging `linear_ramp`
# implementations (a documented defect), and this helper is covered by its own test module.
from export_desktop_no_headwind_csv import load_desktop_headwind_ramp  # noqa: E402

# --- Cycle-scoped configuration (repoint at each roll-forward) -------------------------------
CSV_DIR = "data-official/2026-09/csv"
PUBLISHED_CURVES = "september_canonical_curves.csv"
PUBLISHED_SUMMARY = "september_dec15_summary.csv"

# TODO (2026-09-04 repoint): replace <config>/<slug> with the September canonical desktop build once it exists.
DESKTOP_FORECAST_PATH = (
    "data-official/2026-09/desktop_<config>_2026-09-02/<slug>/"
    "mozaic_daily_forecast.2026-09-02.ld-D.adj-ijlo.parquet"
)
PREV_DESKTOP_FORECAST_PATH = (
    "data-official/2026-08/desktop_g01_2026-08-02/"
    "cps0.1649_thresh032_recent17_cpr0.814_ncp40_clip0.6_sps0.00825_regimemultiplicative/"
    "mozaic_daily_forecast.2026-08-02.ld-D.adj-lo.parquet"
)
CURRENT_ADJUSTMENTS_DIR = "data-official/2026-09/adjustments"
PRIOR_ADJUSTMENTS_DIR = "data-official/2026-08/adjustments"

FORECAST_START = pd.Timestamp("2026-08-02")       # August desktop seam
PREV_FORECAST_START = pd.Timestamp("2026-07-06")  # July's seam
DISPLAY_START = pd.Timestamp("2026-01-01")
DISPLAY_END = pd.Timestamp("2026-12-31")
MEASUREMENT_DATE = pd.Timestamp("2026-12-15")
TROUGH_WINDOW_END = pd.Timestamp("2026-10-15")

EXCLUDED_COUNTRIES = ("IR", "CN")
DESKTOP_SEGMENT = '{"os": "ALL"}'
DESKTOP_APP_NAME = "desktop"
DESKTOP_DATA_SOURCE = "legacy_desktop"
DESKTOP_REQUIRED_STATE = ["l", "o"]

SCOPE_LABEL = "EX_IR_CN"

# Reconciliation tolerance, in DAU. Mozaic's top-down reconciliation allocates fractional DAU, so
# summing 16 float64 columns of ~5e7 leaves ~1e-8 of representation noise (measured: 3.7e-08 on the
# August build, 3.0e-08 on July's). Exact equality is therefore the wrong test. 1e-3 sits five orders
# above the observed noise and six below the 0.5 DAU that could move a published rounded figure, so
# it still fails loudly on a genuine top-down reconciliation break.
RECONCILIATION_TOLERANCE_DAU = 1e-3
# Column/file labels per scope. The actuals column carries only SCOPE_LABEL in both files: actuals
# never carry a headwind, so tagging them NO_WIN10_HEADWIND would assert something meaningless.
SCOPES = [
    {
        "key": "canonical",
        "apply_headwind": True,
        "file_marker": f"DESKTOP_ONLY.{SCOPE_LABEL}",
        "column_suffix": SCOPE_LABEL,
        "ledger_prefix": "win10_headwind_applied",
    },
    {
        "key": "no_headwind",
        "apply_headwind": False,
        "file_marker": f"DESKTOP_ONLY.WIN10_HEADWIND_REMOVED.{SCOPE_LABEL}",
        "column_suffix": f"NO_WIN10_HEADWIND_{SCOPE_LABEL}",
        "ledger_prefix": "win10_headwind_added_back",
    },
]


def load_country_dau(path: str) -> tuple[pd.DataFrame, pd.DatetimeIndex]:
    """World-and-per-country daily desktop DAU from a forecast parquet, plus its training dates.

    Returns a date-indexed frame with one column per country (including `ALL`).
    """
    df, _meta = load_forecast(path, require_state=DESKTOP_REQUIRED_STATE)
    rows = df[
        (df["data_source"] == DESKTOP_DATA_SOURCE)
        & (df["segment"] == DESKTOP_SEGMENT)
        & (df["app_name"] == DESKTOP_APP_NAME)
    ].copy()
    rows["target_date"] = pd.to_datetime(rows["target_date"])

    duplicates = rows.groupby(["target_date", "country"]).size().max()
    if duplicates > 1:
        raise ValueError(
            f"{path}: {duplicates} rows per (date, country) after filtering to "
            f"{DESKTOP_DATA_SOURCE}/{DESKTOP_SEGMENT}/{DESKTOP_APP_NAME}. Summing them would "
            f"double-count; the selector no longer isolates one series."
        )

    pivot = rows.pivot(index="target_date", columns="country", values="dau").sort_index()
    training_dates = pd.DatetimeIndex(
        sorted(rows.loc[(rows["data_type"] == "training") & (rows["country"] == "ALL"),
                        "target_date"].unique())
    )
    return pivot, training_dates


def check_country_reconciliation(pivot: pd.DataFrame, label: str) -> float:
    """`ALL` must equal the sum of the named countries, else `ALL - IR - CN` is not the true ex-scope.

    Returns the observed max residual so the caller can report it.
    """
    named = [c for c in pivot.columns if c != "ALL"]
    missing = [c for c in EXCLUDED_COUNTRIES if c not in named]
    if missing:
        raise ValueError(
            f"{label}: {missing} absent from the parquet's country tiles {sorted(named)}. "
            f"An '{SCOPE_LABEL}' file cannot be produced from a build that does not carry them "
            f"as their own tiles -- they would be hiding inside ROW."
        )
    residual = (pivot["ALL"] - pivot[named].sum(axis=1)).abs().max()
    if residual > RECONCILIATION_TOLERANCE_DAU:
        raise ValueError(
            f"{label}: ALL differs from the sum of its {len(named)} country tiles by up to "
            f"{residual:.6e} DAU, over the {RECONCILIATION_TOLERANCE_DAU:g} tolerance. Mozaic "
            f"reconciles top-down, so ALL - IR - CN is only exact while this residual is float "
            f"noise. Do not ship an {SCOPE_LABEL} file from this build."
        )
    return residual


def scope_daily(pivot: pd.DataFrame) -> pd.Series:
    """Daily DAU with the excluded countries removed."""
    return pivot["ALL"] - pivot[list(EXCLUDED_COUNTRIES)].sum(axis=1)


def forecast_ma(daily: pd.Series, seam: pd.Timestamp) -> pd.Series:
    """Variance-matched 28d MA over training+forecast, exactly as the canonical notebook does it.

    Computed on the differenced DAILY series -- never by subtracting per-country MAs, which the
    splice makes non-linear.
    """
    ma = display_ma(daily.index.to_series(), daily, seam)
    ma.index = daily.index
    return ma


def build_curves(
    august: pd.DataFrame, july: pd.DataFrame, training_dates: pd.DatetimeIndex
) -> tuple[dict[str, pd.Series], dict[str, pd.Series]]:
    """Ex-IR/CN 28d-MA curves (pre-headwind) plus each cycle's headwind ramp."""
    august_daily, july_daily = scope_daily(august), scope_daily(july)

    curves = {
        "actuals": daily_to_28ma(
            pd.Series(training_dates), august_daily.reindex(training_dates)
        ),
        "current_august": forecast_ma(august_daily, FORECAST_START),
        "prior_july": forecast_ma(july_daily, PREV_FORECAST_START),
    }
    ramps = {
        "current_august": load_desktop_headwind_ramp(
            CURRENT_ADJUSTMENTS_DIR, curves["current_august"].index, FORECAST_START),
        "prior_july": load_desktop_headwind_ramp(
            PRIOR_ADJUSTMENTS_DIR, curves["prior_july"].index, PREV_FORECAST_START),
    }
    return curves, ramps


def build_scope_frame(
    scope: dict, curves: dict[str, pd.Series], ramps: dict[str, pd.Series]
) -> pd.DataFrame:
    """One output curve frame, over the display window, rounded to published integers."""
    index = pd.date_range(DISPLAY_START, DISPLAY_END, freq="D")
    suffix = scope["column_suffix"]

    def column(series: pd.Series, first_valid: pd.Timestamp | None = None) -> pd.Series:
        clipped = series.reindex(index)
        if first_valid is not None:
            clipped = clipped.where(clipped.index >= first_valid)
        return clipped.round(0)

    frame = pd.DataFrame({"date": index.strftime("%Y-%m-%d")})
    frame[f"desktop_actuals_{SCOPE_LABEL}"] = column(curves["actuals"]).values
    for family, first_valid in [("prior_july", None), ("current_august", FORECAST_START)]:
        series = curves[family]
        if scope["apply_headwind"]:
            series = series + ramps[family]
        frame[f"desktop_{family}_{suffix}"] = column(series, first_valid).values
    return frame


def build_scope_summary(
    scope: dict, frame: pd.DataFrame, ramps: dict[str, pd.Series]
) -> pd.DataFrame:
    """Dec-15 headline + summer trough, read back off this file's own rounded columns."""
    suffix = scope["column_suffix"]
    indexed = frame.assign(date=pd.to_datetime(frame["date"])).set_index("date")
    current = indexed.loc[MEASUREMENT_DATE, f"desktop_current_august_{suffix}"]
    prior = indexed.loc[MEASUREMENT_DATE, f"desktop_prior_july_{suffix}"]
    window = indexed.loc[
        FORECAST_START:TROUGH_WINDOW_END, f"desktop_current_august_{suffix}"
    ].dropna()
    # Signed as the file needs it read: the canonical scope HAS this applied; the no-headwind scope
    # would need it ADDED BACK to return to the canonical figure.
    sign = 1 if scope["apply_headwind"] else -1
    ledger = scope["ledger_prefix"]
    return pd.DataFrame([{
        "series": "Desktop",
        "measurement_date": MEASUREMENT_DATE.strftime("%Y-%m-%d"),
        f"current_august_{suffix}": int(current),
        f"prior_july_{suffix}": int(prior),
        f"delta_vs_july_{suffix}": int(current - prior),
        f"delta_pct_vs_july_{suffix}": round((current / prior - 1) * 100, 3),
        f"summer_trough_min_{suffix}": int(window.min()),
        f"summer_trough_date_{suffix}": window.idxmin().strftime("%Y-%m-%d"),
        f"{ledger}_august": int(sign * ramps["current_august"][MEASUREMENT_DATE]),
        f"{ledger}_july": int(sign * ramps["prior_july"][MEASUREMENT_DATE]),
    }])


def verify_world_reconstruction(
    august: pd.DataFrame, july: pd.DataFrame, published: pd.DataFrame,
    ramps_index: pd.DatetimeIndex,
) -> None:
    """Rebuild the WORLD curve from the parquets and require it to match the published CSV.

    This is the load-bearing check: it is what licenses trusting the parquet -> display_ma -> ramp
    path at all. If it passes, the only thing separating the ex-IR/CN output from the published
    numbers is the country subtraction.
    """
    for family, daily, seam, adjustments_dir in [
        ("current_august", august["ALL"], FORECAST_START, CURRENT_ADJUSTMENTS_DIR),
        ("prior_july", july["ALL"], PREV_FORECAST_START, PRIOR_ADJUSTMENTS_DIR),
    ]:
        ma = forecast_ma(daily, seam)
        ramp = load_desktop_headwind_ramp(adjustments_dir, ma.index, seam)
        rebuilt = (ma + ramp).round(0).reindex(published.index)
        overlap = rebuilt.notna() & published[f"desktop_{family}"].notna()
        residual = (rebuilt[overlap] - published.loc[overlap, f"desktop_{family}"]).abs()
        assert residual.max() <= 1, (
            f"world {family} rebuilt from the parquet differs from {PUBLISHED_CURVES} by up to "
            f"{residual.max():,.0f} DAU on {(residual > 1).sum()} of {overlap.sum()} rows. The "
            f"reconstruction path is wrong, so the {SCOPE_LABEL} output cannot be trusted either."
        )


def verify_outputs(
    written: dict[str, pd.DataFrame], summaries: dict[str, pd.DataFrame],
    august: pd.DataFrame, july: pd.DataFrame, ramps: dict[str, pd.Series],
    training_dates: pd.DatetimeIndex,
) -> None:
    """Cross-checks between the two scopes and against the excluded countries."""
    canonical, no_headwind = written["canonical"], written["no_headwind"]
    canonical_suffix = SCOPES[0]["column_suffix"]
    no_hw_suffix = SCOPES[1]["column_suffix"]

    # 1. The two scopes must differ by EXACTLY the ramp, on every row.
    for family in ["current_august", "prior_july"]:
        difference = (canonical[f"desktop_{family}_{canonical_suffix}"]
                      - no_headwind[f"desktop_{family}_{no_hw_suffix}"])
        expected = ramps[family].reindex(canonical.index)
        overlap = difference.notna() & expected.notna()
        residual = (difference[overlap] - expected[overlap]).abs()
        assert residual.max() <= 1, (
            f"{family}: the canonical and no-headwind scopes differ by up to "
            f"{residual.max():,.0f} DAU more than the ramp -- one of them is not the other plus `h`."
        )

    # 2. Actuals are identical between the two files and end where the training rows end.
    pd.testing.assert_series_equal(
        canonical[f"desktop_actuals_{SCOPE_LABEL}"], no_headwind[f"desktop_actuals_{SCOPE_LABEL}"],
        check_names=False,
    )
    last_actual = canonical[f"desktop_actuals_{SCOPE_LABEL}"].last_valid_index()
    assert last_actual == training_dates.max(), (
        f"actuals end {last_actual}, but the parquet's last training row is "
        f"{training_dates.max()} -- the actuals column is not the training rows."
    )

    # 3. The excluded countries are genuinely gone: adding their MAs back must recover the world
    #    curve to within the known splice non-linearity, and be far from zero at Dec-15.
    excluded_dec15 = august.loc[MEASUREMENT_DATE, list(EXCLUDED_COUNTRIES)].sum()
    assert excluded_dec15 > 0, "IR+CN daily DAU at Dec-15 is not positive -- wrong columns"
    world_dec15 = forecast_ma(august["ALL"], FORECAST_START)[MEASUREMENT_DATE]
    scope_dec15 = no_headwind.loc[MEASUREMENT_DATE, f"desktop_current_august_{no_hw_suffix}"]
    assert world_dec15 - scope_dec15 > 1_000_000, (
        f"ex-{'/'.join(EXCLUDED_COUNTRIES)} Dec-15 is only {world_dec15 - scope_dec15:,.0f} below "
        f"world; IR+CN are ~5.8% of desktop, so the subtraction did not happen."
    )

    # 4. Schema: desktop only, one scope per file, no world/mobile/ALL columns.
    for key, frame in written.items():
        suffix = next(s["column_suffix"] for s in SCOPES if s["key"] == key)
        assert list(frame.columns) == [
            f"desktop_actuals_{SCOPE_LABEL}",
            f"desktop_prior_july_{suffix}",
            f"desktop_current_august_{suffix}",
        ], f"{key} has unexpected columns {list(frame.columns)}"

    # 5. Each summary re-derives from its own curves file.
    for key, summary in summaries.items():
        suffix = next(s["column_suffix"] for s in SCOPES if s["key"] == key)
        row = summary.set_index("series").loc["Desktop"]
        frame = written[key]
        current = frame.loc[MEASUREMENT_DATE, f"desktop_current_august_{suffix}"]
        prior = frame.loc[MEASUREMENT_DATE, f"desktop_prior_july_{suffix}"]
        assert row[f"current_august_{suffix}"] == current, f"{key} summary disagrees with its curves"
        assert row[f"prior_july_{suffix}"] == prior, f"{key} summary disagrees with its curves"
        assert row[f"delta_vs_july_{suffix}"] == current - prior, (
            f"{key} delta is {row[f'delta_vs_july_{suffix}']:,.0f} but the published columns differ "
            f"by {current - prior:,.0f} -- a reader subtracting them would get another answer."
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv-dir", default=CSV_DIR,
                        help=f"directory holding the published canonical CSVs (default {CSV_DIR})")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the Dec-15 figures without writing files")
    args = parser.parse_args()

    csv_dir = Path(args.csv_dir)
    published = pd.read_csv(csv_dir / PUBLISHED_CURVES, parse_dates=["date"]).set_index("date")

    august, training_dates = load_country_dau(DESKTOP_FORECAST_PATH)
    july, _ = load_country_dau(PREV_DESKTOP_FORECAST_PATH)
    residuals = {
        "August": check_country_reconciliation(august, "August build"),
        "July": check_country_reconciliation(july, "July delivered build"),
    }
    print(f"country tiles reconcile on both builds ({len(august.columns) - 1} tiles + ALL); "
          f"max residual "
          + ", ".join(f"{label} {value:.2e}" for label, value in residuals.items()) + " DAU")

    verify_world_reconstruction(august, july, published, august.index)
    print(f"world curve rebuilt from both parquets matches {PUBLISHED_CURVES}")

    curves, ramps = build_curves(august, july, training_dates)
    frames = {s["key"]: build_scope_frame(s, curves, ramps) for s in SCOPES}
    summaries = {s["key"]: build_scope_summary(s, frames[s["key"]], ramps) for s in SCOPES}

    if args.dry_run:
        for scope in SCOPES:
            print(f"\n--- {scope['file_marker']} ---")
            print(summaries[scope["key"]].to_string(index=False))
        return

    written = {}
    for scope in SCOPES:
        key, marker = scope["key"], scope["file_marker"]
        curves_path = csv_dir / f"{Path(PUBLISHED_CURVES).stem}.{marker}.csv"
        summary_path = csv_dir / f"{Path(PUBLISHED_SUMMARY).stem}.{marker}.csv"
        frames[key].to_csv(curves_path, index=False)
        summaries[key].to_csv(summary_path, index=False)
        print(f"wrote {curves_path}  ({len(frames[key])} rows x {len(frames[key].columns)} cols)")
        print(f"wrote {summary_path}")
        written[key] = pd.read_csv(curves_path, parse_dates=["date"]).set_index("date")
        summaries[key] = pd.read_csv(summary_path)

    verify_outputs(written, summaries, august, july, ramps, training_dates)
    print("\nVerified:")
    print(f"  ALL == sum of country tiles, 0 DAU residual, on both builds")
    print(f"  world reconstruction reproduces the published CSV (licenses the parquet path)")
    print(f"  the two scopes differ by exactly the `h` ramp on every row")
    print(f"  actuals identical across both files, ending at the last training row")
    print(f"  IR+CN removed; desktop only, one scope per file")
    for scope in SCOPES:
        print(f"\n--- {scope['file_marker']} ---")
        print(summaries[scope["key"]].to_string(index=False))


if __name__ == "__main__":
    main()
