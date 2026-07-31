"""Round-1 figures for the Aug-25 gap-narrowing search.

Two panels, because the candidates differ by tens of thousands on a ~45M curve and would
otherwise render as a single line:

  (a) canonical format -- actuals from Jan 1, the s01 curve, July's delivered curve, and the
      candidate envelope, with the Aug-25 target marked.
  (b) each candidate's deviation from s01 over time, which is the only view at which a
      ~20K parameter effect is legible.

Writes plots/round1_gradient.png.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import FuncFormatter

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from mozaic_daily.adjustments import load_forecast  # noqa: E402
from mozaic_daily.seam_ma import display_ma  # noqa: E402

from score_gradient import (  # noqa: E402
    AUG25_TARGET, AUG25_TOLERANCE, KNOBS, S01_PARQUET, _config_of, _identify,
)

HERE = Path(__file__).resolve().parent
PLOTS = HERE / "plots"
FORECAST_START = pd.Timestamp("2026-07-28")
DISPLAY_START = pd.Timestamp("2026-01-01")
DISPLAY_END = pd.Timestamp("2026-12-31")
AUG25 = pd.Timestamp("2026-08-25")
HEADWIND = REPO_ROOT / "data-official/2026-08/adjustments/headwind.json"


def curve(parquet: Path) -> tuple[pd.Series, pd.Series]:
    """Return (post-headwind 28d-MA over the display window, actuals daily)."""
    df, _ = load_forecast(str(parquet))
    mask = (df["country"] == "ALL") & (df["segment"] == '{"os": "ALL"}')
    sub = df.loc[mask].copy()
    sub["target_date"] = pd.to_datetime(sub["target_date"])
    sub = sub.sort_values("target_date")

    ma = display_ma(sub["target_date"], sub["dau"], FORECAST_START)

    spec = json.loads(HEADWIND.read_text())
    start, anchor = pd.Timestamp(spec["start_date"]), pd.Timestamp(spec["anchor_date"])
    elapsed = pd.Series((ma.index - start).days, index=ma.index).clip(lower=0)
    ramp = (spec["desktop_dau"] * elapsed / (anchor - start).days)
    ma = ma + ramp.where(ma.index >= FORECAST_START, 0.0)

    actuals = sub.loc[sub["data_type"] == "training"].set_index("target_date")["dau"]
    actuals_ma = actuals.rolling(28).mean()
    return ma.loc[DISPLAY_START:DISPLAY_END], actuals_ma.loc[DISPLAY_START:]


def main() -> int:
    PLOTS.mkdir(exist_ok=True)

    s01, actuals = curve(S01_PARQUET)
    candidates = {}
    for parquet in sorted(HERE.glob("runs/*/mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet")):
        candidates[_identify(_config_of(parquet))] = curve(parquet)[0]

    july = pd.read_csv(
        REPO_ROOT / "data-official/2026-07/csv/july_canonical_curves.csv", parse_dates=["date"]
    ).set_index("date")["desktop_current_july"]

    fig, (top, bottom) = plt.subplots(
        2, 1, figsize=(15, 11), gridspec_kw={"height_ratios": [1.35, 1]}
    )

    # -- (a) canonical format -----------------------------------------------------------
    top.plot(actuals.index, actuals.values, color="black", lw=2.2, label="Actuals (28d MA)")
    for i, (label, c) in enumerate(candidates.items()):
        top.plot(c.index, c.values, color="tab:orange", lw=0.9, alpha=0.55,
                 label="Round-1 candidates (10)" if i == 0 else None)
    top.plot(s01.index, s01.values, color="tab:blue", lw=2.4, label="s01 (current canonical)")
    top.plot(july.index, july.values, color="tab:green", lw=1.8, ls="--",
             label="July delivered")
    top.plot([AUG25], [AUG25_TARGET], marker="*", ms=20, color="crimson", ls="none", zorder=6,
             label=f"Aug-25 target {AUG25_TARGET:,.0f}")
    top.axvline(FORECAST_START, color="grey", lw=1, ls=":")
    top.set_title("August desktop DAU, 28-day MA (post-headwind) — Round-1 ±δ candidates\n"
                  "All 10 candidates sit far above the target; they overlay s01 at this scale",
                  fontsize=13)

    # -- (b) deviation from s01 ---------------------------------------------------------
    palette = plt.cm.tab10.colors
    knob_color = {k: palette[i] for i, k in enumerate(KNOBS)}
    for label, c in candidates.items():
        knob = label.rsplit("_", 1)[0]
        bottom.plot(c.index, (c - s01).values, lw=1.5, color=knob_color[knob],
                    ls="--" if label.endswith("_lo") else "-", label=label)
    bottom.axhline(0, color="black", lw=1)
    bottom.axhline(AUG25_TARGET - float(s01.loc[AUG25]), color="crimson", lw=1.6, ls=":",
                   label=f"required at Aug-25 ({AUG25_TARGET - float(s01.loc[AUG25]):,.0f})")
    bottom.axvline(AUG25, color="grey", lw=1, ls=":")
    bottom.set_ylim(-320_000, 320_000)
    bottom.set_title("Deviation from s01 — the required move is ~7× the largest single-knob effect",
                     fontsize=13)
    bottom.legend(ncol=4, fontsize=8, loc="lower left")

    for ax in (top, bottom):
        ax.set_xlim(DISPLAY_START, DISPLAY_END)
        ax.grid(alpha=0.3)
    # Two decimals: adjacent ticks are ~0.5M apart on a 43-49M range, so whole-million
    # labels would render several identical "45M" ticks.
    top.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e6:.2f}M"))
    bottom.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e3:+,.0f}K"))
    top.legend(loc="lower right", fontsize=10)

    fig.tight_layout()
    out = PLOTS / "round1_gradient.png"
    fig.savefig(out, dpi=140, bbox_inches="tight")
    print(f"Wrote {out}")

    band = (AUG25_TARGET - AUG25_TOLERANCE, AUG25_TARGET + AUG25_TOLERANCE)
    print(f"s01 at Aug-25: {float(s01.loc[AUG25]):,.0f}   accept band: "
          f"{band[0]:,.0f}-{band[1]:,.0f}")
    for label, c in sorted(candidates.items(), key=lambda kv: float(kv[1].loc[AUG25])):
        print(f"  {label:<10} {float(c.loc[AUG25]):>12,.0f}  "
              f"delta vs s01 {float(c.loc[AUG25]) - float(s01.loc[AUG25]):>+9,.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
