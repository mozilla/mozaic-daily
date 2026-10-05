"""Build the September 2026 paid-DAU curve that `p` consumes as its paid forecast.

Source: the marketing team's GMIO widget query (``source_data/query_gmio_paid_dau_total_all.sql``,
template params resolved to metric = 'Total Paid DAU', country = 'All'), saved verbatim as
``source_data/gmio_paid_dau_total_all.20260904.csv``: 51 weekly ISO-Monday rows with four
presentation columns (UAC actual / forecast, UAC+Meta actual / forecast; Meta is stacked
cumulatively on UAC).

Composition rule (Brendan, 2026-09-04): where the UAC+Meta line is present use it, otherwise
the UAC-only line — for actuals and for forecast alike. Every value in ``paid_dau_used`` is one of
the four query columns verbatim; ``basis`` names which.

Then August's method, unchanged (``../../2026-08/marketing/august_marketing_lift.ipynb``):
  - each weekly value sits on its Monday; linear interpolation to daily; forward-fill after the
    last Monday (2026-12-21) through 2026-12-31;
  - lift(d) = level(d) - level(2026-03-30), 0 before the anchor. ``p`` adds the anchor back
    (``paid_forecast.anchor_paid_dau``), so the parquet stays in the lift-plus-anchor framing
    August settled on. The level itself is also written, for inspection, as
    ``paid_dau_level_daily``.

Outputs (this directory):
    marketing_lift_model.gmio_uac_meta_total.2026-09-02.parquet   index target_date;
        marketing_lift_daily, marketing_lift_ma, paid_dau_level_daily
    marketing_lift_model.gmio_uac_meta_total.2026-09-02.meta.json  provenance + key_values
    paid_dau_curve.2026-09-02.xlsx   three sheets: raw query, composed weekly, daily
    plots/paid_dau_curve.2026-09-02.png

This is a reproducible producer, not throwaway. Re-run after re-pulling the query; the applier
contract is unchanged. Not wired until data-official/2026-09/organic/organic.json exists and
points at the parquet (the `p` split must be rebuilt for the September training window first).
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
SOURCE_CSV = HERE / "source_data" / "gmio_paid_dau_total_all.20260904.csv"
SOURCE_SQL = HERE / "source_data" / "query_gmio_paid_dau_total_all.sql"
FORECAST_START = "2026-09-02"
STEM = f"marketing_lift_model.gmio_uac_meta_total.{FORECAST_START}"
OUT_PARQUET = HERE / f"{STEM}.parquet"
OUT_META = HERE / f"{STEM}.meta.json"
OUT_XLSX = HERE / f"paid_dau_curve.{FORECAST_START}.xlsx"
OUT_PLOT = HERE / "plots" / f"paid_dau_curve.{FORECAST_START}.png"

ANCHOR_DATE = pd.Timestamp("2026-03-30")      # August's method-D anchor: last Monday before the 2026-04-06 launch
DAILY_END = pd.Timestamp("2026-12-31")        # the curve's horizon; `p` holds flat past it
MA_WINDOW = 28
KPI_DATE = pd.Timestamp("2026-12-15")
# Reference: what August's curve gave `p` (organic.json anchor + meta key_values.lift_dec15).
AUGUST_ANCHOR = 922250.4715237124
AUGUST_LIFT_DEC15 = 637226.7399246667


def compose_weekly(raw: pd.DataFrame) -> pd.DataFrame:
    """One value per week from the four presentation columns, with its provenance."""
    weekly = raw.copy()
    weekly["date"] = pd.to_datetime(weekly["date"])
    weekly = weekly.sort_values("date").reset_index(drop=True)

    def pick(row) -> tuple[float, str, bool]:
        for column, is_actual in (("uac_meta_actual", True), ("uac_actual", True),
                                  ("uac_meta_forecast", False), ("uac_forecast", False)):
            if pd.notna(row[column]):
                return float(row[column]), column, is_actual
        raise ValueError(f"week {row['date'].date()} has no value in any of the four columns")

    picked = weekly.apply(pick, axis=1, result_type="expand")
    weekly["paid_dau_used"], weekly["basis"], weekly["is_actual"] = picked[0], picked[1], picked[2]

    # The handoff week is emitted on both lines; they must agree or the composition is ambiguous.
    overlap = weekly.dropna(subset=["uac_meta_actual", "uac_meta_forecast"])
    if not overlap.empty and (overlap["uac_meta_actual"] != overlap["uac_meta_forecast"]).any():
        raise ValueError("UAC+Meta actual and forecast disagree on the handoff week")
    if weekly["date"].diff().dropna().dt.days.ne(7).any():
        raise ValueError("weeks are not consecutive Mondays")
    return weekly


def interpolate_weekly_to_daily(weekly: pd.DataFrame) -> pd.Series:
    """Monday values -> daily by linear interpolation, forward-filled to DAILY_END (August's rule)."""
    series = pd.Series(weekly["paid_dau_used"].to_numpy(), index=pd.DatetimeIndex(weekly["date"]), dtype="float64")
    daily_index = pd.date_range(series.index.min(), DAILY_END, freq="D", name="target_date")
    return series.reindex(daily_index).interpolate(method="linear", limit_area="inside").ffill()


def lift_from_level(level: pd.Series) -> tuple[pd.Series, float]:
    anchor = float(level.loc[ANCHOR_DATE])
    lift = (level - anchor).where(level.index >= ANCHOR_DATE, 0.0)
    assert lift.loc[ANCHOR_DATE] == 0.0 and lift.notna().all()
    return lift.astype("float64"), anchor


def write_workbook(raw: pd.DataFrame, weekly: pd.DataFrame, daily: pd.DataFrame) -> None:
    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as book:
        raw.to_excel(book, sheet_name="raw_query", index=False)
        weekly.assign(date=weekly["date"].dt.date).to_excel(book, sheet_name="composed_weekly", index=False)
        daily.reset_index().assign(target_date=lambda d: d["target_date"].dt.date).to_excel(book, sheet_name="daily", index=False)


def write_plot(weekly: pd.DataFrame, daily: pd.DataFrame) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    fig, (ax_level, ax_lift) = plt.subplots(2, 1, figsize=(12, 8), sharex=True, facecolor="#fcfcfb")
    for ax in (ax_level, ax_lift):
        ax.set_facecolor("#fcfcfb")
        ax.grid(alpha=0.25)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e6:.2f}M"))
    ax_level.plot(daily.index, daily["paid_dau_level_daily"], color="#2a78d6", lw=1.6, label="daily level (interpolated)")
    actual = weekly[weekly["is_actual"]]
    forecast = weekly[~weekly["is_actual"]]
    ax_level.scatter(actual["date"], actual["paid_dau_used"], color="#2a78d6", s=18, zorder=3, label="weekly, actual")
    ax_level.scatter(forecast["date"], forecast["paid_dau_used"], facecolors="none", edgecolors="#2a78d6", s=22, zorder=3, label="weekly, forecast")
    ax_level.axhline(AUGUST_ANCHOR + AUGUST_LIFT_DEC15, color="#52514e", ls="--", lw=1, label="August's Dec-15 paid level")
    ax_level.set_ylabel("paid DAU level")
    ax_level.legend(loc="upper left", fontsize=9, frameon=False)
    ax_level.set_title("September 2026 paid-DAU curve for `p` — GMIO feed, UAC+Meta where present else UAC", loc="left")

    ax_lift.plot(daily.index, daily["marketing_lift_daily"], color="#eb6834", lw=1.6, label=f"lift = level − level({ANCHOR_DATE.date()})")
    ax_lift.axhline(0, color="#52514e", lw=0.8)
    ax_lift.set_ylabel("lift vs anchor")
    ax_lift.legend(loc="upper left", fontsize=9, frameon=False)
    for ax in (ax_level, ax_lift):
        for when, text in ((ANCHOR_DATE, "anchor"), (pd.Timestamp(FORECAST_START), "seam"), (KPI_DATE, "Dec-15")):
            ax.axvline(when, color="#52514e", ls=":", lw=1)
            ax.text(when, 0.02, f" {text}", transform=ax.get_xaxis_transform(), fontsize=9, color="#52514e")
    fig.tight_layout()
    fig.savefig(OUT_PLOT, dpi=130, facecolor="#fcfcfb")
    plt.close(fig)


def main() -> None:
    raw = pd.read_csv(SOURCE_CSV)
    weekly = compose_weekly(raw)
    level = interpolate_weekly_to_daily(weekly)
    lift, anchor = lift_from_level(level)
    daily = pd.DataFrame({
        "marketing_lift_daily": lift,
        "marketing_lift_ma": lift.rolling(MA_WINDOW, min_periods=14).mean().astype("float64"),
        "paid_dau_level_daily": level.astype("float64"),
    })
    daily.index.name = "target_date"
    daily.to_parquet(OUT_PARQUET)
    write_workbook(raw, weekly, daily)
    write_plot(weekly, daily)

    last_actual_week = weekly.loc[weekly["is_actual"], "date"].max()
    meta = {
        "model_name": "fenix_marketing_lift_gmio_uac_meta_total",
        "description": ("September 2026 paid-DAU curve for the `p` paid/organic split, anchor-and-subtract on the total Paid DAU "
                        "basis exactly as August: lift(d) = paid_dau(d) - paid_dau(2026-03-30), 0 before the anchor. Source is the "
                        "marketing team's GMIO cross-channel feed (UAC + Meta Android, Meta stacked cumulatively); composed as "
                        "UAC+Meta where present else UAC, for actuals and forecast alike. The level is also written."),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "mozaic_daily_git_hash": subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True, text=True).stdout.strip(),
        "forecast_start_date": FORECAST_START,
        "coverage": {"start_date": str(level.index.min().date()), "end_date": str(DAILY_END.date()),
                     "anchor_date": str(ANCHOR_DATE.date()), "actuals_through_week_of": str(last_actual_week.date()),
                     "last_weekly_row": str(weekly["date"].max().date()), "tail_rule": "forward-fill after the last Monday to 2026-12-31; `p` holds flat past that"},
        "methodology": {"framing": "anchor-and-subtract on the marketing team's paid-DAU curve (August's method D, unchanged)",
                        "composition": "COALESCE(uac_meta_actual, uac_actual) for actual weeks; COALESCE(uac_meta_forecast, uac_forecast) for forecast weeks",
                        "weekly_to_daily": "value on its ISO Monday, linear interpolation, forward-fill after the last Monday",
                        "metric_basis": "total", "meta_channel": "included, stacked cumulatively, assumed fully incremental",
                        "iran": "not in the feed (50 countries, no IR row), so ex-IR by construction"},
        "source_data": {"query_csv": str(SOURCE_CSV.relative_to(REPO)), "query_csv_sha1": hashlib.sha1(SOURCE_CSV.read_bytes()).hexdigest(),
                        "query_sql": str(SOURCE_SQL.relative_to(REPO)), "query_sql_sha1": hashlib.sha1(SOURCE_SQL.read_bytes()).hexdigest(),
                        "feed_tables": ["mozdata.analysis.ahe_gmio_weekly_paid_dau_views_20260901"], "pulled_on": "2026-09-04",
                        "weekly_rows": int(len(weekly)), "basis_counts": weekly["basis"].value_counts().to_dict()},
        "key_values": {"anchor_paid_dau": anchor, "lift_dec15": float(lift.loc[KPI_DATE]), "lift_year_end": float(lift.loc[DAILY_END]),
                       "level_dec15": float(level.loc[KPI_DATE]), "level_year_end": float(level.loc[DAILY_END]),
                       "august_level_dec15": AUGUST_ANCHOR + AUGUST_LIFT_DEC15,
                       "level_dec15_change_vs_august": float(level.loc[KPI_DATE]) - (AUGUST_ANCHOR + AUGUST_LIFT_DEC15)},
        "known_limitations": [
            "The feed's forecast reflects a spend plan (+36%/week UAC from the previous feed) and a refit; the change vs August is mostly plan, not actuals revision.",
            "Weekly values are treated as the level on their Monday and interpolated; within-week shape is not observed.",
            "Curve ends 2026-12-31; `p` holds it flat through 2027 by its tail_policy.",
            "Not wired until data-official/2026-09/organic/organic.json points at this parquet with anchor_paid_dau from key_values.",
        ],
        "artifact_sha1": hashlib.sha1(OUT_PARQUET.read_bytes()).hexdigest(),
    }
    OUT_META.write_text(json.dumps(meta, indent=2) + "\n")

    print(f"Wrote {OUT_PARQUET.name}, {OUT_META.name}, {OUT_XLSX.name}, {OUT_PLOT.relative_to(HERE)}")
    print(f"  weeks={len(weekly)} ({weekly['date'].min().date()} -> {weekly['date'].max().date()}), "
          f"actuals through week of {last_actual_week.date()}, basis counts {meta['source_data']['basis_counts']}")
    print(f"  anchor {ANCHOR_DATE.date()} level = {anchor:,.0f}  (August's anchor {AUGUST_ANCHOR:,.0f})")
    for d in ["2026-09-02", "2026-12-15", "2026-12-31"]:
        ts = pd.Timestamp(d)
        print(f"  {d}: level {level.loc[ts]:>12,.0f}   lift {lift.loc[ts]:>12,.0f}")
    print(f"  Dec-15 level change vs August: {meta['key_values']['level_dec15_change_vs_august']:+,.0f}")


if __name__ == "__main__":
    main()
