#!/usr/bin/env python3
"""Verify the launch-on-login (`l`) desktop overlay end-to-end.

Produces the three ALL-desktop DAU curves the plan calls for, reusing the cached
raw legacy_desktop DAU query part (no BigQuery):

  (a) raw / no-l           — forecast on original training (LOL left implicit)
  (b) subtract-and-forecast — forecast on subtracted training, NO add-back
  (c) full subtract+add     — (b) then add the capped curve back  == the adj-l output

and a conservatism-margin plot ("how wrong is our assumption"): the flat 125K vs
the measured excess vs the ~220K convolution model.

Run:  source .venv/bin/activate && python scripts/verify_lol_overlay.py
Writes plots + a numbers JSON to data-official/2026-07/launch_on_login/plots/.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from mozaic_daily.adjustments import (  # noqa: E402
    add_lift_to_forecast, compute_country_shares, load_lift_series,
    load_overlay_spec, subtract_lift_from_training,
)
from mozaic_daily.config import get_runtime_config  # noqa: E402
from mozaic_daily.data import get_aggregate_data, get_queries  # noqa: E402
from mozaic_daily.forecast import get_desktop_forecast_dfs  # noqa: E402
from mozaic_daily.queries import ADDITIONAL_HOLIDAYS, DataSource, Metric  # noqa: E402
from mozaic_daily.tables import combine_tables  # noqa: E402

FORECAST_START = "2026-06-29"
MEASURE = pd.Timestamp("2026-12-15")
OUT = REPO / "data-official" / "2026-07" / "launch_on_login" / "plots"
OUT.mkdir(parents=True, exist_ok=True)
LOL_REPO = Path.home() / "work" / "launch-on-login"


def world_ma28(df_combined: pd.DataFrame) -> pd.Series:
    """28d-MA of the ALL-country / ALL-segment daily DAU, indexed by date."""
    w = df_combined[(df_combined["country"] == "ALL") & (df_combined["population"] == "ALL")].copy()
    w["target_date"] = pd.to_datetime(w["target_date"])
    s = w.sort_values("target_date").set_index("target_date")["DAU"].astype(float)
    return s.rolling(28, min_periods=28).mean()


def run_forecast(source_data: dict) -> pd.DataFrame:
    cfg = get_runtime_config(forecast_start_date_override=FORECAST_START)
    result = get_desktop_forecast_dfs(
        source_data, cfg["forecast_start_date"], cfg["forecast_end_date"],
        additional_holidays=ADDITIONAL_HOLIDAYS.get(DataSource.LEGACY_DESKTOP, []),
        data_source=DataSource.LEGACY_DESKTOP.value,
    )
    return combine_tables(result.dfs)


def main() -> None:
    cfg = get_runtime_config(forecast_start_date_override=FORECAST_START)
    queries = get_queries(
        cfg["country_string"],
        data_source_filter={DataSource.LEGACY_DESKTOP},
        metric_filter={Metric.DAU},
    )
    datasets = get_aggregate_data(queries, project="mozdata", checkpoints=True, clean=False)
    source_data = {m: df.assign(x=pd.to_datetime(df["x"])) for m, df in datasets["desktop"]["legacy"].items()}

    spec_path = REPO / "data-official" / "2026-07" / "launch_on_login" / "lol.json"
    spec = load_overlay_spec(spec_path)
    lift = load_lift_series(spec, spec_path.parent)
    shares = compute_country_shares(
        source_data[Metric.DAU.value], training_end_date=pd.Timestamp(cfg["training_end_date"]),
        window_days=spec["allocation"]["window_days"], flag_column=spec["allocation"]["flag_column"],
    )

    # Cache the three MA series so re-plotting never re-runs the (slow) forecasts.
    series_cache = OUT.parent / "lol_three_curves.parquet"
    if series_cache.exists():
        print(f"Loading cached curves from {series_cache} (skip forecasts)", flush=True)
        cached = pd.read_parquet(series_cache)
        a, b, c = cached["a_raw"], cached["b_subtract_only"], cached["c_full"]
    else:
        print("[1/2] forecasting RAW (no-l) ...", flush=True)
        raw_combined = run_forecast(source_data)

        print("[2/2] forecasting on SUBTRACTED training ...", flush=True)
        subtracted = dict(source_data)
        subtracted[Metric.DAU.value] = subtract_lift_from_training(
            source_data[Metric.DAU.value], daily_lift_series=lift, country_shares=shares,
            flag_column=spec["allocation"]["flag_column"], sentinel_attr="launch_on_login_subtracted",
        )
        sub_combined = run_forecast(subtracted)
        full_combined = add_lift_to_forecast(
            sub_combined, daily_lift_series=lift, country_shares=shares,
            forecast_start=pd.Timestamp(FORECAST_START), metric_column=Metric.DAU.value,
            population_value=spec["allocation"]["flag_column"],
        )

        a = world_ma28(raw_combined)      # raw / no-l
        b = world_ma28(sub_combined)      # subtract-only
        c = world_ma28(full_combined)     # full subtract+add
        pd.DataFrame({"a_raw": a, "b_subtract_only": b, "c_full": c}).to_parquet(series_cache)

    nums = {
        "dec15_raw_no_l": float(a.loc[MEASURE]),
        "dec15_subtract_only": float(b.loc[MEASURE]),
        "dec15_full_subtract_add": float(c.loc[MEASURE]),
        "dec15_c_minus_a": float(c.loc[MEASURE] - a.loc[MEASURE]),
        "dec15_a_minus_b": float(a.loc[MEASURE] - b.loc[MEASURE]),
        "dec15_c_minus_b": float(c.loc[MEASURE] - b.loc[MEASURE]),
    }
    print(json.dumps(nums, indent=2))
    (OUT / "lol_three_curve_numbers.json").write_text(json.dumps(nums, indent=2, sort_keys=True))

    # --- Plot 1: three-curve isolation (levels + delta panel) ---
    # The three levels differ by ~0.1M on a ~49M axis (0.2%), so they overlap
    # visually; the bottom panel shows the deltas that carry the whole story.
    plot_from = pd.Timestamp(FORECAST_START)
    fig, (ax, axd) = plt.subplots(
        2, 1, figsize=(12, 8), sharex=True, gridspec_kw={"height_ratios": [2, 1]}
    )
    for series, label, color in [
        (a, "(a) raw / no-l", "tab:gray"),
        (b, "(b) subtract-and-forecast (no add-back)", "tab:orange"),
        (c, "(c) full subtract+add  (= adj-l output)", "tab:blue"),
    ]:
        s = series[series.index >= plot_from]
        ax.plot(s.index, s.values / 1e6, label=label, color=color, lw=2)
    ax.axvline(MEASURE, color="tab:red", ls=":", lw=1, label="Dec-15 KPI")
    ax.set_ylabel("ALL-desktop DAU, 28d-MA (M)")
    ax.set_title("Launch-on-login overlay — three-curve isolation (legacy_desktop DAU)")
    ax.legend(loc="upper right", fontsize=9)
    ax.grid(alpha=0.3)

    ca = (c - a)[(c - a).index >= plot_from]
    ab = (a - b)[(a - b).index >= plot_from]
    cb = (c - b)[(c - b).index >= plot_from]
    axd.plot(ca.index, ca.values / 1e3, color="tab:blue", lw=2,
             label="(c)-(a): net LOL effect on KPI")
    axd.plot(ab.index, ab.values / 1e3, color="tab:gray", lw=1.8, ls="--",
             label="(a)-(b): LOL Prophet already extrapolated (avoided double-count)")
    axd.plot(cb.index, cb.values / 1e3, color="tab:blue", lw=1, ls=":",
             label="(c)-(b): flat add-back (= 125K cap)")
    axd.axvline(MEASURE, color="tab:red", ls=":", lw=1)
    dec = MEASURE
    axd.annotate(f"Dec-15: net +{(c[dec]-a[dec])/1e3:.0f}K  (add-back 125K − {(a[dec]-b[dec])/1e3:.0f}K already in raw)",
                 xy=(dec, (c[dec]-a[dec]) / 1e3), xytext=(pd.Timestamp("2026-08-01"), 60),
                 fontsize=9, arrowprops=dict(arrowstyle="->"))
    axd.set_ylabel("Δ 28d-MA (thousands)")
    axd.legend(loc="upper right", fontsize=8)
    axd.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUT / "lol_three_curve_isolation.png", dpi=130)

    # --- Plot 2: conservatism margin ("how wrong is our assumption") ---
    fig2, ax2 = plt.subplots(figsize=(12, 6))
    lol_daily = lift[(lift.index >= "2026-05-01") & (lift.index <= "2026-12-31")]
    ax2.plot(lol_daily.index, lol_daily.values / 1e3, color="tab:red", lw=2.5,
             label="shipped LOL curve (flat 125K)")
    # Measured excess (recompute quick from the tailwind producer's inputs if present)
    conv_csv = LOL_REPO / "forecasts" / "lol_dau_tailwind.csv"
    if conv_csv.exists():
        conv = pd.read_csv(conv_csv, parse_dates=["date"]).set_index("date")
        conv = conv[(conv.index >= "2026-05-01") & (conv.index <= "2026-12-31")]
        ax2.plot(conv.index, conv["delta_dau_ma28"].values / 1e3, color="tab:purple", lw=2,
                 label="convolution model (28d-MA, ~220K Dec-15)")
        gap = float(conv.loc[conv.index.asof(MEASURE), "delta_dau_ma28"]) - 125_000
        ax2.annotate(f"Dec-15 conservatism ≈ {gap/1e3:.0f}K",
                     xy=(MEASURE, 125), xytext=(pd.Timestamp("2026-09-01"), 180),
                     fontsize=10, arrowprops=dict(arrowstyle="->"))
        ax2.fill_between(conv.index, 125, conv["delta_dau_ma28"].values / 1e3,
                         where=(conv["delta_dau_ma28"].values / 1e3 > 125), alpha=0.15,
                         color="tab:purple", label="excluded upside (conservatism band)")
    ax2.axhline(125, color="tab:red", ls=":", lw=1)
    ax2.set_ylabel("desktop DAU tailwind (thousands)")
    ax2.set_title("LOL conservatism margin — shipped 125K vs modeled potential")
    ax2.legend(loc="upper left", fontsize=9)
    ax2.grid(alpha=0.3)
    fig2.tight_layout()
    fig2.savefig(OUT / "lol_conservatism_margin.png", dpi=130)

    print(f"Plots + numbers written to {OUT}")


if __name__ == "__main__":
    main()
