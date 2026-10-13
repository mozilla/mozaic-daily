#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build the China MozillaOnline MAU offset and the population split for the Glean-history `o` spec.

Implements ``data-official/2027-glean-draft/mozillaonline/MAU_OFFSET_APPROACH.md`` exactly: measure China's
June 2026 step per OS population on DAU and MAU in the export, derive the MAU world curve as the DAU curve's
trailing 28-day mean times the step's MAU/DAU ratio, derive per-metric population shares from the steps, write
the MAU curve + meta, update the spec, and run the checks (no step after subtraction, round trip, sanity)
through the real overlay appliers. Raises on any failed check; writes a check plot.

Usage:
    python scripts/build_glean_history_cn_offsets.py            # build + check + update spec
    python scripts/build_glean_history_cn_offsets.py --check-only
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

repo_root = Path(__file__).resolve().parent.parent
if str(repo_root / "src") not in sys.path:
    sys.path.insert(0, str(repo_root / "src"))

from mozaic_daily.adjustments import (  # noqa: E402
    add_lift_to_forecast, fixed_country_shares_from_spec, load_lift_series, subtract_lift_from_training,
)
from mozaic_daily.glean_history import OS_FLAGS, RAW_CHECKPOINT_TEMPLATE  # noqa: E402

DRAFT = repo_root / "data-official/2027-glean-draft"
SPEC_DIR = DRAFT / "mozillaonline"
SPEC_PATH = SPEC_DIR / "mozillaonline.glean_history.json"
EXPORT_DIR = DRAFT / "inputs/legacy_to_glean_export"
RAW_DIR = DRAFT / "raw_glean_history_2026-10-09"
MAU_CURVE_FILE = f"mozillaonline_migration_mau.{date.today().isoformat()}.parquet"
MAU_VALUE_COLUMN = "mozillaonline_migration_mau_daily"

# Step measurement windows (approach §"What the history says").
PRE = {"DAU": ("2026-04-15", "2026-05-31"), "MAU": ("2026-05-15", "2026-05-31")}
POST = {"DAU": ("2026-08-01", "2026-09-08"), "MAU": ("2026-08-15", "2026-09-08")}
CHECK_POST = ("2026-07-15", "2026-09-08")      # window for the "no step after subtraction" check
MAX_RESIDUAL_STEP = 0.15
RATIO_BOUNDS = (1.0, 8.0)
CELL_PREFIX = {"windows": "win", "mac": "mac", "linux": "linux"}


def _sha1(path: Path) -> str:
    return hashlib.sha1(path.read_bytes()).hexdigest()


def measure_steps(export_dir: Path) -> dict:
    """China's step per population and metric, the ratios, and the population shares."""
    out = {"steps": {}, "ratios": {}, "population_shares": {}}
    export = {m: pd.read_parquet(export_dir / f"glean_equivalent_{m.lower()}_history_cells_fc15.parquet") for m in ("DAU", "MAU")}
    for m in export.values():
        m["submission_date"] = pd.to_datetime(m["submission_date"])
    for metric in ("DAU", "MAU"):
        out["steps"][metric] = {}
        for pop in OS_FLAGS:
            s = export[metric][export[metric]["cell"] == f"{CELL_PREFIX[pop]}_cn"].set_index("submission_date")["glean_equivalent"]
            pre = float(s[PRE[metric][0]:PRE[metric][1]].mean())
            post = float(s[POST[metric][0]:POST[metric][1]].mean())
            out["steps"][metric][pop] = {"pre": pre, "post": post, "step": post - pre}
    for pop in OS_FLAGS:
        d, m = out["steps"]["DAU"][pop]["step"], out["steps"]["MAU"][pop]["step"]
        out["ratios"][pop] = (m / d) if d > 0 and m > 0 else None
    total_d = sum(max(v["step"], 0.0) for v in out["steps"]["DAU"].values())
    total_m = sum(max(out["steps"]["MAU"][p]["step"], 0.0) for p in OS_FLAGS if out["steps"]["DAU"][p]["step"] > 0)
    out["ratio_world"] = total_m / total_d
    for metric in ("DAU", "MAU"):
        clipped = {p: max(out["steps"][metric][p]["step"], 0.0) if out["steps"]["DAU"][p]["step"] > 0 else 0.0 for p in OS_FLAGS}
        tot = sum(clipped.values())
        out["population_shares"][metric] = {p: round(v / tot, 4) for p, v in clipped.items() if v > 0}
        # make the rounded shares sum to exactly 1 on the largest population
        largest = max(out["population_shares"][metric], key=out["population_shares"][metric].get)
        out["population_shares"][metric][largest] = round(1.0 - sum(v for p, v in out["population_shares"][metric].items() if p != largest), 4)
    return out


def build_mau_curve(dau_curve: pd.Series, ratio_world: float) -> pd.Series:
    """MAU world curve: trailing 28-day mean of the DAU world curve times the step's MAU/DAU ratio."""
    mau = dau_curve.sort_index().rolling(28, min_periods=1).mean() * ratio_world
    mau.name = MAU_VALUE_COLUMN
    return mau


def china_training(raw_dir: Path, metric: str) -> pd.DataFrame:
    df = pd.read_parquet(raw_dir / RAW_CHECKPOINT_TEMPLATE.format(metric=metric))
    df["x"] = pd.to_datetime(df["x"])
    return df


def run_checks(spec: dict, measurements: dict, raw_dir: Path, plot_path: Path) -> dict:
    """Apply the real subtract / add-back to the raw frames and assert the approach's checks."""
    results = {}
    fig, axes = plt.subplots(2, 2, figsize=(15, 9))
    for i, metric in enumerate(("DAU", "MAU")):
        curve_spec = spec["metrics"][metric]
        series = load_lift_series(curve_spec, SPEC_DIR)
        frame = china_training(raw_dir, metric)
        present = set(frame["country"].unique())
        shares = fixed_country_shares_from_spec(spec, present)
        subtracted = frame
        for pop, pshare in spec["allocation"]["population_shares"][metric].items():
            subtracted = subtract_lift_from_training(subtracted, daily_lift_series=series * pshare, country_shares=shares,
                                                     flag_column=pop, sentinel_attr=f"check:{metric}:{pop}")
        for j, pop in enumerate(("windows", "mac")):
            raw = frame[(frame["country"] == "CN") & frame[pop]].set_index("x")["y"].astype(float)
            after = subtracted[(subtracted["country"] == "CN") & subtracted[pop]].set_index("x")["y"].astype(float)
            pre = after[PRE[metric][0]:PRE[metric][1]].mean()
            post = after[CHECK_POST[0]:CHECK_POST[1]].mean()
            residual = post / pre - 1
            raw_step = raw[CHECK_POST[0]:CHECK_POST[1]].mean() / raw[PRE[metric][0]:PRE[metric][1]].mean() - 1
            results[f"{metric}:{pop}"] = {"raw_step_pct": round(raw_step * 100, 2), "residual_step_pct": round(residual * 100, 2)}
            if abs(residual) >= MAX_RESIDUAL_STEP:
                raise AssertionError(f"{metric} CN {pop}: residual step after subtraction {residual:+.1%} exceeds {MAX_RESIDUAL_STEP:.0%}")
            ax = axes[i, j]
            win = slice("2025-07-01", "2026-10-09")
            ax.plot(raw[win].index, raw[win].values, color="#888", lw=1, label="raw export (Glean-equivalent)")
            ax.plot(after[win].index, after[win].values, color="#d62728", lw=1.2, label="after o subtraction")
            ax.axvspan(pd.Timestamp(PRE[metric][0]), pd.Timestamp(PRE[metric][1]), color="#1f77b4", alpha=0.1)
            ax.axvspan(pd.Timestamp(CHECK_POST[0]), pd.Timestamp(CHECK_POST[1]), color="#2ca02c", alpha=0.1)
            ax.set_title(f"CN {pop} {metric}: raw step {raw_step:+.0%}, residual {residual:+.1%}", fontsize=11)
            ax.grid(alpha=0.3)
            ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v / 1e6:.2f}M" if v >= 1e5 else f"{v / 1e3:.0f}K"))
            if i == 0 and j == 0:
                ax.legend(fontsize=8)
        # Round trip: add every piece back on a forecast-shaped frame built from the subtracted training rows.
        shaped = subtracted.rename(columns={"x": "target_date"}).copy()
        shaped["population"] = [next(p for p in OS_FLAGS if row[p]) for _, row in shaped[list(OS_FLAGS)].iterrows()] if False else \
            shaped[list(OS_FLAGS)].idxmax(axis=1)
        shaped[metric] = shaped["y"].astype(float)
        shaped["source"] = "actual"
        for pop, pshare in spec["allocation"]["population_shares"][metric].items():
            shaped = add_lift_to_forecast(shaped, daily_lift_series=series * pshare, country_shares=shares,
                                          forecast_start=pd.Timestamp("2026-10-10"), metric_column=metric, population_value=pop)
        merged = shaped[shaped["country"] == "CN"].merge(frame[frame["country"] == "CN"], left_on=["target_date", *OS_FLAGS],
                                                          right_on=["x", *OS_FLAGS], suffixes=("", "_raw"))
        worst = float((merged[metric] - merged["y_raw"].astype(float)).abs().max())
        results[f"{metric}:round_trip_max_abs"] = worst
        if worst > 1.0:
            raise AssertionError(f"{metric}: round trip differs from raw by up to {worst:.2f} on a CN row")
    fig.suptitle("China offsets check: blue = pre window, green = post window; after subtraction the step must be gone (|residual| < 15%)")
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    plot_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(plot_path, dpi=100)
    plt.close(fig)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()

    spec = json.loads(SPEC_PATH.read_text())
    measurements = measure_steps(EXPORT_DIR)
    print(json.dumps(measurements, indent=2))
    for pop, r in measurements["ratios"].items():
        if r is not None and not (RATIO_BOUNDS[0] <= r <= RATIO_BOUNDS[1]):
            raise AssertionError(f"ratio for {pop} = {r:.2f} outside {RATIO_BOUNDS}")

    if not args.check_only:
        dau_curve = load_lift_series({"data_file": spec["data_file"], "value_column": spec["value_column"]}, SPEC_DIR)
        mau_curve = build_mau_curve(dau_curve, measurements["ratio_world"])
        if (mau_curve < 0).any() or mau_curve.isna().any():
            raise AssertionError("MAU curve has negative or NaN values")
        if (mau_curve[: "2026-05-31"] != 0).any():
            raise AssertionError("MAU curve is non-zero before 2026-06-01")
        out = pd.DataFrame({MAU_VALUE_COLUMN: mau_curve})
        out.index.name = "target_date"
        out.to_parquet(SPEC_DIR / MAU_CURVE_FILE)
        meta = {
            "producer": "scripts/build_glean_history_cn_offsets.py",
            "approach": "mozillaonline/MAU_OFFSET_APPROACH.md",
            "built_on": date.today().isoformat(),
            "dau_curve_file": spec["data_file"], "dau_curve_sha1": _sha1(SPEC_DIR / spec["data_file"]),
            "export_sha1": {m: _sha1(EXPORT_DIR / f"glean_equivalent_{m.lower()}_history_cells_fc15.parquet") for m in ("DAU", "MAU")},
            "construction": "trailing 28-day mean of the DAU world curve x ratio_world; held flat where the DAU curve is",
            "measurement_windows": {"pre": PRE, "post": POST},
            "measurements": measurements,
            "dec15_2027_mau_offset_world": float(mau_curve.get(pd.Timestamp("2027-12-15"), float("nan"))),
            "derived_not_measured": "Nobody has measured MozillaOnline MAU directly; this curve is the DAU curve scaled by China's own step ratio.",
        }
        (SPEC_DIR / MAU_CURVE_FILE.replace(".parquet", ".meta.json")).write_text(json.dumps(meta, indent=2) + "\n")
        spec["metrics"] = {
            "DAU": {"data_file": spec["data_file"], "value_column": spec["value_column"]},
            "MAU": {"data_file": MAU_CURVE_FILE, "value_column": MAU_VALUE_COLUMN},
        }
        spec["allocation"]["population_shares"] = measurements["population_shares"]
        spec["notes"] = ("MAU OFFSET + POPULATION SPLIT added 2026-10-12 (MAU_OFFSET_APPROACH.md): `metrics` carries the DAU curve and a derived "
                         f"MAU curve (28d mean of DAU x {measurements['ratio_world']:.3f}); `allocation.population_shares` splits each metric's curve "
                         "across windows/mac by China's measured June-2026 step. || " + spec["notes"])
        SPEC_PATH.write_text(json.dumps(spec, indent=2) + "\n")
        print(f"wrote {SPEC_DIR / MAU_CURVE_FILE} and updated {SPEC_PATH}")

    spec = json.loads(SPEC_PATH.read_text())
    results = run_checks(spec, measurements, RAW_DIR, DRAFT / "plots" / "cn_offsets_check.png")
    (SPEC_DIR / "cn_offsets_measurements.json").write_text(json.dumps({"measurements": measurements, "checks": results}, indent=2) + "\n")
    print("checks:", json.dumps(results, indent=2))
    print("all checks passed")


if __name__ == "__main__":
    main()
