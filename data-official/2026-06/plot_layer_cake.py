"""Render the "anatomy of one published curve" layer-cake teaching plots.

Onboarding charts for a new analyst: the published DAU forecast is not raw model
output but a stack of deliberate adjustments. One panel per platform, three
curves + two shaded bands each. The vertical distance between "raw" and
"published" is hand-built judgment — that gap is the point.

The two platforms tell mirror-image stories, which is itself the lesson:

    DESKTOP   raw  − headwind ramp (Win10 EOL)  + synthetic Iran  → published
    MOBILE    raw  + marketing lift (Fenix)      + synthetic Iran  → published

Shared visual language so the two read together: grey dashed = raw model output,
green = synthetic Iran / published. The middle adjustment is platform-specific
(red = headwind subtracted, blue = marketing added).

Layer sources (all curves are 28-day MAs, already seam-smoothed):
  - Desktop raw  = sum of per-country `*.no-headwinds.csv` (no adjustments).
  - Desktop mid  = canonical `desktop_current_june_no_iran` (headwind baked in).
  - Mobile mid   = canonical `mobile_current_june_no_iran` (marketing baked
                   per-tile in the parquet; headwind −27k is negligible at 17M
                   scale and folded into "raw").
  - Mobile raw   = mid − the marketing-lift 28dMA read straight from the spec
                   parquet (`marketing_lift_ma`), since no raw mobile parquet
                   exists at the canonical 2026-05-26 date to diff against.
  - Both published = `*_current_june_plus_iran`.

    source .venv/bin/activate && python3 data-official/2026-06/plot_layer_cake.py

Writes:
    csv/plots/layer_cake_desktop.png
    csv/plots/layer_cake_mobile.png
"""

import glob
import os
import subprocess

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

ALL_CSV = "data-official/2026-06/csv/june_canonical_curves.csv"
PER_COUNTRY_GLOB = "data-official/2026-06/csv/per_country/june_canonical_curves.*.no-headwinds.csv"
MARKETING_PARQUET = "data-official/2026-06/marketing/marketing_lift_model.real_data_v2.hybrid.2026-05-22.parquet"
PLOTS_DIR = "data-official/2026-06/csv/plots"
DEC15 = pd.Timestamp("2026-12-15")

RAW_STYLE = dict(color="#7a7a7a", lw=1.4, ls="--")
PUBLISHED_STYLE = dict(color="#1e7d34", lw=2.6, ls="-")
IRAN_FILL = "#1e7d34"


def millions_formatter(x, _pos):
    # Narrow ranges (desktop ~45-50M, mobile ~14-17.5M): two decimals keep
    # adjacent ticks distinct rather than collapsing to identical "16M / 16M".
    return f"{x / 1e6:.2f}M"


def desktop_layers():
    allc = pd.read_csv(ALL_CSV, parse_dates=["date"]).set_index("date")
    per_country = glob.glob(PER_COUNTRY_GLOB)
    raw = sum(
        pd.read_csv(f, parse_dates=["date"]).set_index("date")["desktop_current_june"]
        for f in per_country
    ).dropna()
    return {
        "actuals": allc["desktop_actuals_excl_ir"].dropna(),
        "raw": raw,
        "mid": allc["desktop_current_june_no_iran"].dropna(),
        "published": allc["desktop_current_june_plus_iran"].dropna(),
        "mid_label": "2. − headwind ramp (Win10 EOL)",
        "mid_color": "#c0392b",
        "headwind_band": ("headwind subtracted", "#c0392b"),
    }


def mobile_layers():
    allc = pd.read_csv(ALL_CSV, parse_dates=["date"]).set_index("date")
    mid = allc["mobile_current_june_no_iran"].dropna()
    # No raw mobile parquet at 2026-05-26 to diff, so read the marketing lift's
    # own 28dMA from the spec parquet and back it out of the published no-Iran
    # curve. (mid = raw + marketing − tiny headwind, so mid − marketing ≈ raw.)
    marketing_ma = pd.read_parquet(MARKETING_PARQUET)["marketing_lift_ma"]
    raw = (mid - marketing_ma.reindex(mid.index)).dropna()
    return {
        "actuals": allc["mobile_actuals_excl_ir"].dropna(),
        "raw": raw,
        "mid": mid,
        "published": allc["mobile_current_june_plus_iran"].dropna(),
        "mid_label": "2. + marketing lift (Fenix campaign)",
        "mid_color": "#2c5fa8",
        "headwind_band": ("marketing lift added", "#2c5fa8"),
    }


def render(layers, platform, legend_loc):
    actuals, raw, mid, published = (
        layers["actuals"], layers["raw"], layers["mid"], layers["published"]
    )
    fig, ax = plt.subplots(figsize=(14, 7))

    ax.plot(actuals.index, actuals.values, color="black", lw=1.6,
            label="Actuals (excl. Iran)")
    ax.plot(raw.index, raw.values, label="1. Raw model output", **RAW_STYLE)
    ax.plot(mid.index, mid.values, color=layers["mid_color"], lw=1.4, ls="--",
            label=layers["mid_label"])
    ax.plot(published.index, published.values,
            label="3. + synthetic Iran  →  PUBLISHED", **PUBLISHED_STYLE)

    # Shade the two adjustment bands over the shared forecast horizon. fill_between
    # fills regardless of which curve is on top (headwind pulls down, marketing
    # pushes up), so the same call works for both platforms.
    band_x = mid.index
    raw_b = raw.reindex(band_x)
    pub_b = published.reindex(band_x)
    mid_adj = abs(raw_b.loc[DEC15] - mid.loc[DEC15]) / 1e6
    iran_adj = abs(pub_b.loc[DEC15] - mid.loc[DEC15]) / 1e6
    band_name, band_color = layers["headwind_band"]
    ax.fill_between(band_x, mid.values, raw_b, color=band_color, alpha=0.12,
                    label=f"{band_name} ({mid_adj:.2f}M @ Dec-15)")
    ax.fill_between(band_x, mid.values, pub_b, color=IRAN_FILL, alpha=0.12,
                    label=f"synthetic Iran added (+{iran_adj:.2f}M @ Dec-15)")

    ax.axvline(DEC15, color="red", ls=":", alpha=0.5)
    for series, color, va in [
        (raw, RAW_STYLE["color"], "bottom"),
        (published, PUBLISHED_STYLE["color"], "center"),
        (mid, layers["mid_color"], "top"),
    ]:
        val = series.reindex([DEC15]).iloc[0]
        ax.annotate(f"{val/1e6:.2f}M", (DEC15, val), xytext=(8, 0),
                    textcoords="offset points", color=color, fontsize=9,
                    fontweight="bold", va=va)

    ax.set_title(
        f"Anatomy of one published curve — 2026 {platform.title()} DAU (28-day MA)\n"
        "Published forecast = raw model output + deliberate adjustments",
        fontsize=13,
    )
    ax.set_ylabel("DAU")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(millions_formatter))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    ax.legend(loc=legend_loc, fontsize=9)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    path = f"{PLOTS_DIR}/layer_cake_{platform}.png"
    fig.savefig(path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return path


def main():
    git_root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
    ).stdout.strip()
    os.chdir(git_root)
    os.makedirs(PLOTS_DIR, exist_ok=True)
    print(f"Wrote {render(desktop_layers(), 'desktop', 'lower left')}")
    print(f"Wrote {render(mobile_layers(), 'mobile', 'lower right')}")


if __name__ == "__main__":
    main()
