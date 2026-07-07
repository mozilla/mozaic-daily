#!/usr/bin/env python3
"""Verify the MozillaOnline (`o`) desktop migration overlay end-to-end.

Mirrors scripts/verify_lol_overlay.py: produces three ALL-desktop DAU 28d-MA
curves that isolate `o` alone (no `l`), reusing the cached raw legacy_desktop
DAU query:

  (a) raw / no-o            — forecast on original training
  (b) subtract-and-forecast — forecast on subtracted training, NO add-back
  (c) full subtract+add     — (b) then add the migration curve back == adj-o

The Dec-15 deltas answer the double-count question exactly as `l` did:
  (c)-(a) = net `o` effect on the KPI (expected WELL BELOW the raw ~567K curve,
            because the June migration ramp is already in training and Prophet
            extrapolates part of it)
  (a)-(b) = how much migration Prophet already extrapolated
  (c)-(b) = the full add-back (the migration curve level)

Run:  source .venv/bin/activate && python scripts/verify_mozillaonline_overlay.py
Writes plots + a numbers JSON to data-official/2026-07/mozillaonline/plots/.
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
    add_lift_to_forecast, fixed_country_shares_from_spec, load_lift_series,
    load_overlay_spec, subtract_lift_from_training,
)
from mozaic_daily.config import get_runtime_config  # noqa: E402
from mozaic_daily.data import get_aggregate_data, get_queries  # noqa: E402
from mozaic_daily.forecast import get_desktop_forecast_dfs  # noqa: E402
from mozaic_daily.queries import ADDITIONAL_HOLIDAYS, DataSource, Metric  # noqa: E402
from mozaic_daily.tables import combine_tables  # noqa: E402

FORECAST_START = "2026-06-29"
MEASURE = pd.Timestamp("2026-12-15")
OUT = REPO / "data-official" / "2026-07" / "mozillaonline" / "plots"
OUT.mkdir(parents=True, exist_ok=True)


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

    spec_path = REPO / "data-official" / "2026-07" / "mozillaonline" / "mozillaonline.json"
    spec = load_overlay_spec(spec_path)
    lift = load_lift_series(spec, spec_path.parent)
    flag_column = spec["allocation"]["flag_column"]
    dau_train = source_data[Metric.DAU.value]
    present = dau_train.loc[dau_train[flag_column] == True, "country"].unique()  # noqa: E712
    shares = fixed_country_shares_from_spec(spec, present)

    series_cache = OUT.parent / "mozillaonline_three_curves.parquet"
    if series_cache.exists():
        print(f"Loading cached curves from {series_cache} (skip forecasts)", flush=True)
        cached = pd.read_parquet(series_cache)
        a, b, c = cached["a_raw"], cached["b_subtract_only"], cached["c_full"]
    else:
        print("[1/2] forecasting RAW (no-o) ...", flush=True)
        raw_combined = run_forecast(source_data)

        print("[2/2] forecasting on SUBTRACTED training ...", flush=True)
        subtracted = dict(source_data)
        subtracted[Metric.DAU.value] = subtract_lift_from_training(
            dau_train, daily_lift_series=lift, country_shares=shares,
            flag_column=flag_column, sentinel_attr="mozillaonline_subtracted",
        )
        sub_combined = run_forecast(subtracted)
        full_combined = add_lift_to_forecast(
            sub_combined, daily_lift_series=lift, country_shares=shares,
            forecast_start=pd.Timestamp(FORECAST_START), metric_column=Metric.DAU.value,
            population_value=flag_column,
        )

        a = world_ma28(raw_combined)
        b = world_ma28(sub_combined)
        c = world_ma28(full_combined)
        pd.DataFrame({"a_raw": a, "b_subtract_only": b, "c_full": c}).to_parquet(series_cache)

    nums = {
        "dec15_raw_no_o": float(a.loc[MEASURE]),
        "dec15_subtract_only": float(b.loc[MEASURE]),
        "dec15_full_subtract_add": float(c.loc[MEASURE]),
        "dec15_c_minus_a": float(c.loc[MEASURE] - a.loc[MEASURE]),
        "dec15_a_minus_b": float(a.loc[MEASURE] - b.loc[MEASURE]),
        "dec15_c_minus_b": float(c.loc[MEASURE] - b.loc[MEASURE]),
        "dec15_migration_curve_28ma": float(lift.rolling(1).mean().loc[MEASURE]),
    }
    print(json.dumps(nums, indent=2))
    (OUT / "mozillaonline_three_curve_numbers.json").write_text(json.dumps(nums, indent=2, sort_keys=True))

    # --- Three-curve isolation (levels + delta panel) ---
    plot_from = pd.Timestamp(FORECAST_START)
    fig, (ax, axd) = plt.subplots(
        2, 1, figsize=(12, 8), sharex=True, gridspec_kw={"height_ratios": [2, 1]}
    )
    for series, label, color in [
        (a, "(a) raw / no-o", "tab:gray"),
        (b, "(b) subtract-and-forecast (no add-back)", "tab:orange"),
        (c, "(c) full subtract+add  (= adj-o output)", "tab:blue"),
    ]:
        s = series[series.index >= plot_from]
        ax.plot(s.index, s.values / 1e6, label=label, color=color, lw=2)
    ax.axvline(MEASURE, color="tab:red", ls=":", lw=1, label="Dec-15 KPI")
    ax.set_ylabel("ALL-desktop DAU, 28d-MA (M)")
    ax.set_title("MozillaOnline overlay — three-curve isolation (legacy_desktop DAU)")
    ax.legend(loc="upper right", fontsize=9)
    ax.grid(alpha=0.3)

    ca = (c - a)[(c - a).index >= plot_from]
    ab = (a - b)[(a - b).index >= plot_from]
    cb = (c - b)[(c - b).index >= plot_from]
    axd.plot(ca.index, ca.values / 1e3, color="tab:blue", lw=2,
             label="(c)-(a): net MozillaOnline effect on KPI")
    axd.plot(ab.index, ab.values / 1e3, color="tab:gray", lw=1.8, ls="--",
             label="(a)-(b): migration Prophet already extrapolated (avoided double-count)")
    axd.plot(cb.index, cb.values / 1e3, color="tab:blue", lw=1, ls=":",
             label="(c)-(b): full add-back (migration curve level)")
    axd.axvline(MEASURE, color="tab:red", ls=":", lw=1)
    dec = MEASURE
    axd.annotate(
        f"Dec-15: net +{(c[dec]-a[dec])/1e3:.0f}K  "
        f"(add-back {(c[dec]-b[dec])/1e3:.0f}K − {(a[dec]-b[dec])/1e3:.0f}K already in raw)",
        xy=(dec, (c[dec]-a[dec]) / 1e3), xytext=(pd.Timestamp("2026-08-01"), (c[dec]-a[dec]) / 1e3 * 0.6),
        fontsize=9, arrowprops=dict(arrowstyle="->"))
    axd.set_ylabel("Δ 28d-MA (thousands)")
    axd.legend(loc="upper right", fontsize=8)
    axd.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(OUT / "mozillaonline_three_curve_isolation.png", dpi=130)

    # --- Migration curve for context ---
    fig2, ax2 = plt.subplots(figsize=(12, 5))
    mo = lift[(lift.index >= "2026-05-01") & (lift.index <= "2027-03-31")]
    ax2.plot(mo.index, mo.values / 1e3, color="tab:green", lw=1.2, alpha=0.6, label="daily migration DAU")
    ax2.plot(mo.index, mo.rolling(28, min_periods=1).mean().values / 1e3, color="tab:green", lw=2.5,
             label="28d-MA")
    ax2.axvline(MEASURE, color="tab:red", ls=":", lw=1)
    ax2.set_ylabel("MozillaOnline migration DAU (thousands)")
    ax2.set_title("MozillaOnline official migration curve (held flat ~550K into 2027)")
    ax2.legend(loc="upper right", fontsize=9)
    ax2.grid(alpha=0.3)
    fig2.tight_layout()
    fig2.savefig(OUT / "mozillaonline_migration_curve.png", dpi=130)

    print(f"Plots + numbers written to {OUT}")


if __name__ == "__main__":
    main()
