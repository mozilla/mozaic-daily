"""April backtest — the decision gate for the variance-matched seam transition.

Read-only, parquet-only, NO BigQuery. The realized April actuals live in the *June*
parquet's training rows (data_type='training' spans 2026-04-01..2026-05-25), so we can
score the April forecast's seam transition against what actually happened — without a
model re-run or a BQ query.

For each country, over the April forecast's transition window (forecast days 1..27 =
calendar 2026-04-01..2026-04-27; April seam = 2026-04-01) we compare three 28dMA series:

  realized : 28dMA of the *real* daily DAU (June-parquet training rows), the transition
             truth the April forecast was trying to predict.
  OLD      : the shipped straight linear-bridge transition (_display_ma_linear, pinned
             here so it survives the export rewrite), run on the April forecast parquet.
  NEW      : the variance-matched transition (export.display_ma), same April parquet.

Decision metric is the BIAS-REMOVED (shape) MAE: subtract each series' own window mean
before comparing, so a constant offset (April's forecast *level* error, not the transition
*method*) does not count against either method.

GATE — the global (ALL-level) transition (user decision 2026-05-29):
  The main deliverable is the global ALL-level forecast curve (the headline
  june_canonical_curves.csv / stakeholder plots), so the gate is decided on the ALL-level
  transition, not per country:
    - PRIMARY: NEW shape-MAE < OLD shape-MAE for the DESKTOP ALL-level transition by a
      clear margin (>= GLOBAL_MARGIN).
    - SECONDARY (sanity): the MOBILE ALL-level transition is not materially harmed
      (NEW shape-MAE within GLOBAL_TOLERANCE of OLD, or better).
  The per-country table is still printed (and AR/US/IN/BR overlays saved) as a diagnostic,
  but per-country shape regressions (e.g. desktop IN, where the April forecast's own trend
  curvature diverged from realized) do NOT gate: they net out in the ALL aggregate, which is
  what stakeholders see.

    source .venv/bin/activate && python3 research/ma-seam-turbulence/backtest_seam.py
"""

import importlib.util
import os
import subprocess

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

GIT_ROOT = subprocess.run(
    ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
).stdout.strip()
os.chdir(GIT_ROOT)

EXPORT_PY = "data-official/2026-06/export_canonical_curves.py"
_spec = importlib.util.spec_from_file_location("export_canonical_curves", EXPORT_PY)
export = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(export)

APRIL_SEAM = pd.Timestamp("2026-04-01")
WINDOW = 28
TRANSITION_END = APRIL_SEAM + pd.Timedelta(days=WINDOW - 2)  # day 27 = 2026-04-27 inclusive
OUT_DIR = "research/ma-seam-turbulence"

# Global (ALL-level) gate thresholds.
GLOBAL_MARGIN = 0.05      # desktop ALL NEW must beat OLD shape-MAE by at least this fraction
GLOBAL_TOLERANCE = 0.05   # mobile ALL NEW may be at most this fraction worse than OLD (sanity)

# Per-country countries flagged in the diagnostic table (informational only — not gating).
STRESS = ["AR", "BR", "IN"]
CONTROLS = ["US", "CA"]

# Desktop = primary gate. Mobile = secondary sanity check.
# Realized truth must be REAL actuals: use a `.raw.` June parquet's training rows. The
# 2026-05-26 mobile build is only available as `.adj-m.` (marketing-lift subtracts lift
# from Fenix training rows from 2026-04-06 on, contaminating April actuals), so mobile's
# realized truth uses the clean 2026-05-21 `.raw.` build instead.
PLATFORMS = {
    "desktop": {
        "april_parquet": "data-official/2026-04/desktop_cps0.15983_thresh050_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.ld-D.raw.parquet",
        "realized_parquet": "data-official/2026-06/desktop_cps0.15983_thresh050_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-26.ld-D.raw.parquet",
        "segment": '{"os": "ALL"}',
        "data_source": "legacy_desktop",
        "app_name": "desktop",
    },
    "mobile": {
        "april_parquet": "data-official/2026-04/mobile_cps0.02_thresh32_recent13_clip0.6/mozaic_daily_forecast.2026-04-01.gm-D.raw.parquet",
        "realized_parquet": "data-official/2026-06/mobile_cps0.02_thresh32_recent13_clip0.6_cap426/mozaic_daily_forecast.2026-05-21.gm-D.raw.parquet",
        "segment": "{}",
        "data_source": "glean_mobile",
        "app_name": "ALL MOBILE",
    },
}


def _display_ma_linear(dates, values, forecast_start, window=28):
    """PINNED copy of the OLD shipped straight linear-bridge display_ma.

    Kept verbatim here so the backtest can compare against the pre-rewrite behavior even
    after export.display_ma is replaced. Do NOT refactor to share code with export — its
    whole purpose is to be a frozen reference.
    """
    s = pd.Series(values.values, index=pd.to_datetime(dates.values)).sort_index()
    blend_ma = s.rolling(window).mean()
    forecast_only_ma = s[s.index >= forecast_start].rolling(window).mean()
    first_clean_date = forecast_start + pd.Timedelta(days=window - 1)

    result = blend_ma.copy()
    result.loc[forecast_only_ma.index] = forecast_only_ma

    if forecast_start in blend_ma.index and first_clean_date in forecast_only_ma.index:
        anchor_value = blend_ma.loc[forecast_start]
        clean_value = forecast_only_ma.loc[first_clean_date]
        span_days = (first_clean_date - forecast_start).days
        bridge_mask = (result.index >= forecast_start) & (result.index < first_clean_date)
        elapsed = (result.index - forecast_start).days
        bridge = anchor_value + (clean_value - anchor_value) * elapsed / span_days
        result[bridge_mask] = bridge[bridge_mask]
    return result


def load_country(parquet, cfg, country, training_only=False):
    """Load one country's daily DAU from a forecast parquet, sorted by date."""
    df = pd.read_parquet(parquet)
    mask = (
        (df["country"] == country)
        & (df["segment"] == cfg["segment"])
        & (df["data_source"] == cfg["data_source"])
        & (df["app_name"] == cfg["app_name"])
    )
    if training_only:
        mask &= df["data_type"] == "training"
    sub = df.loc[mask, ["target_date", "dau"]].copy()
    sub["target_date"] = pd.to_datetime(sub["target_date"])
    return sub.sort_values("target_date").reset_index(drop=True)


def transition_slice(series):
    """Slice a date-indexed MA to the April transition window (days 1..27)."""
    return series[(series.index >= APRIL_SEAM) & (series.index <= TRANSITION_END)]


def shape_mae(estimate, truth):
    """Bias-removed MAE: subtract each series' own mean over the window before comparing."""
    est = estimate - estimate.mean()
    tru = truth - truth.mean()
    return (est - tru).abs().mean()


def evaluate_platform(platform, cfg, plot_countries=("AR", "US")):
    """Return a per-country DataFrame of raw/shape MAE for OLD vs NEW, and save overlay PNGs."""
    countries = sorted(
        c for c in pd.read_parquet(cfg["april_parquet"])["country"].unique() if c != "ALL"
    )
    rows = []
    overlays = {}
    for country in countries:
        april = load_country(cfg["april_parquet"], cfg, country)
        realized_daily = load_country(cfg["realized_parquet"], cfg, country, training_only=True)
        if april.empty or realized_daily.empty:
            continue

        old = transition_slice(_display_ma_linear(april["target_date"], april["dau"], APRIL_SEAM))
        new = transition_slice(export.display_ma(april["target_date"], april["dau"], APRIL_SEAM))
        realized = transition_slice(
            export.daily_to_28ma(realized_daily["target_date"], realized_daily["dau"])
        )

        common = old.index.intersection(new.index).intersection(realized.dropna().index)
        if len(common) < WINDOW - 1:
            continue
        old, new, realized = old[common], new[common], realized[common]

        rows.append({
            "country": country,
            "raw_mae_old": (old - realized).abs().mean(),
            "raw_mae_new": (new - realized).abs().mean(),
            "shape_mae_old": shape_mae(old, realized),
            "shape_mae_new": shape_mae(new, realized),
        })
        if country in plot_countries:
            overlays[country] = (realized, old, new)

    table = pd.DataFrame(rows)
    table["shape_improvement_pct"] = 100 * (
        1 - table["shape_mae_new"] / table["shape_mae_old"]
    )
    for country, (realized, old, new) in overlays.items():
        _save_overlay(platform, country, realized, old, new)
    return table


def _save_overlay(platform, country, realized, old, new):
    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.plot(realized.index, realized.values, color="black", lw=2.4, label="Realized 28dMA (truth)")
    ax.plot(old.index, old.values, color="crimson", lw=1.8, ls="--", label="OLD straight bridge")
    ax.plot(new.index, new.values, color="green", lw=1.8, ls="-.", label="NEW variance-matched")
    ax.set_title(f"April backtest — {platform} {country}: transition vs realized (days 1-27)")
    ax.set_ylabel("DAU")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=9)
    fig.tight_layout()
    path = f"{OUT_DIR}/plots/april_backtest_{platform}_{country}.png"
    fig.savefig(path, dpi=110, bbox_inches="tight")
    plt.close(fig)


def evaluate_all_level(platform, cfg):
    """ALL-level (global) transition: OLD vs NEW shape/raw MAE vs realized. Saves overlay PNG.

    Uses the no-Iran ALL aggregate from both the April forecast parquet and the realized
    June parquet's training rows — a like-for-like global transition (synthetic Iran is a
    separately-composed series, not a model transition, so it is excluded here).
    """
    april = load_country(cfg["april_parquet"], cfg, "ALL")
    realized_daily = load_country(cfg["realized_parquet"], cfg, "ALL", training_only=True)

    old = transition_slice(_display_ma_linear(april["target_date"], april["dau"], APRIL_SEAM))
    new = transition_slice(export.display_ma(april["target_date"], april["dau"], APRIL_SEAM))
    realized = transition_slice(
        export.daily_to_28ma(realized_daily["target_date"], realized_daily["dau"])
    )
    common = old.index.intersection(new.index).intersection(realized.dropna().index)
    old, new, realized = old[common], new[common], realized[common]

    _save_overlay(platform, "ALL", realized, old, new)
    return {
        "platform": platform,
        "raw_mae_old": (old - realized).abs().mean(),
        "raw_mae_new": (new - realized).abs().mean(),
        "shape_mae_old": shape_mae(old, realized),
        "shape_mae_new": shape_mae(new, realized),
    }


def main():
    os.makedirs(f"{OUT_DIR}/plots", exist_ok=True)

    # --- Per-country diagnostic tables (informational only — do NOT gate) ---
    all_tables = []
    for platform, cfg in PLATFORMS.items():
        print(f"\n=== {platform.upper()} per-country (diagnostic, non-gating) ===")
        table = evaluate_platform(platform, cfg)
        table.insert(0, "platform", platform)
        all_tables.append(table)
        with pd.option_context("display.float_format", lambda v: f"{v:,.1f}"):
            print(table.drop(columns="platform").to_string(index=False))

    # --- Global (ALL-level) gate ---
    print("\n" + "=" * 70)
    print("GLOBAL (ALL-level) transition — the decision gate")
    print("=" * 70)
    global_rows = {p: evaluate_all_level(p, cfg) for p, cfg in PLATFORMS.items()}
    for platform, row in global_rows.items():
        improvement = 1 - row["shape_mae_new"] / row["shape_mae_old"]
        raw_impr = 1 - row["raw_mae_new"] / row["raw_mae_old"]
        print(
            f"  {platform:8s} ALL  shape MAE {row['shape_mae_old']:>10,.0f} -> {row['shape_mae_new']:>10,.0f}"
            f"  ({improvement:+.1%})   raw MAE {row['raw_mae_old']:>10,.0f} -> {row['raw_mae_new']:>10,.0f}"
            f"  ({raw_impr:+.1%})"
        )

    desktop_impr = 1 - global_rows["desktop"]["shape_mae_new"] / global_rows["desktop"]["shape_mae_old"]
    mobile_impr = 1 - global_rows["mobile"]["shape_mae_new"] / global_rows["mobile"]["shape_mae_old"]
    desktop_ok = desktop_impr >= GLOBAL_MARGIN
    mobile_ok = mobile_impr >= -GLOBAL_TOLERANCE
    print(
        f"\n  PRIMARY  desktop ALL shape improvement {desktop_impr:+.1%} "
        f"(need >= +{GLOBAL_MARGIN:.0%}): {'PASS' if desktop_ok else 'FAIL'}"
    )
    print(
        f"  SANITY   mobile  ALL shape improvement {mobile_impr:+.1%} "
        f"(allow >= -{GLOBAL_TOLERANCE:.0%}): {'PASS' if mobile_ok else 'FAIL'}"
    )
    passed = desktop_ok and mobile_ok

    # Persist: per-country diagnostics + the gating ALL-level rows.
    combined = pd.concat(all_tables, ignore_index=True)
    global_df = pd.DataFrame(list(global_rows.values()))
    global_df.insert(1, "country", "ALL")
    global_df["shape_improvement_pct"] = 100 * (1 - global_df["shape_mae_new"] / global_df["shape_mae_old"])
    combined = pd.concat([global_df, combined], ignore_index=True)
    csv_path = f"{OUT_DIR}/plots/april_backtest.csv"
    combined.to_csv(csv_path, index=False)
    print(f"\nWrote {csv_path} ({len(combined)} rows; ALL-level rows first)")

    print("\n" + "=" * 70)
    if passed:
        print("DECISION GATE (global ALL-level): PASSED — ship the variance-matched transition (A).")
    else:
        print("DECISION GATE (global ALL-level): FAILED — STOP. Check with the user before any "
              "pivot to the Option C spline fallback. Do NOT auto-switch.")
    print("=" * 70)


if __name__ == "__main__":
    main()
