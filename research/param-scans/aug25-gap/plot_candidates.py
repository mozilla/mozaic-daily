"""Standalone comparison: the two candidate configs vs the locked build, July, and actuals.

Reproducible script, not a notebook cell -- re-run it any time to regenerate the figure.

Curves (desktop ALL, 28-day MA via display_ma, post-headwind):
  actuals          real telemetry through the 2026-07-28 seam
  s01 LOCKED       data-official/2026-08/desktop_locked/  -- the current canonical build
  STAGED           cps 0.2049 / cpr 0.814 / ncp 40 / recent 14 / sps 0.00825
                   6.58% of the gap, Dec-15 +380, kink 1.50x. Gentle slope: 2 of its 6 measured
                   neighbours are actually deeper, so small parameter shifts cost little.
  SPIKE            cps 0.1649 / cpr 0.814 / ncp 40 / recent 17 / sps 0.00825
                   9.52% of the gap, Dec-15 -31,357, kink 1.73x. A true local minimum: all 7
                   measured neighbours are 52,092-165,860 shallower, so the depth is fragile to
                   any future shift in an effective parameter.
  July delivered   data-official/2026-07/csv/july_canonical_curves.csv

The two candidates are the live choice; the 6.78% "alternative" was dropped from this figure.

Three panels, because the candidates differ from the locked build by ~130K on a ~45M curve and are
indistinguishable at full-year scale:
  (a) full-year canonical view
  (b) near-horizon zoom, Jul-Oct, where the whole difference lives
  (c) deviation from the locked build, which is the only view that separates the two candidates

Writes plots/candidates_vs_locked.png.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import FuncFormatter

REPO_ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from mozaic_daily.adjustments import load_forecast  # noqa: E402
from mozaic_daily.seam_ma import display_ma  # noqa: E402

FORECAST_START = pd.Timestamp("2026-07-28")
DISPLAY_START = pd.Timestamp("2026-01-01")
DISPLAY_END = pd.Timestamp("2026-12-31")
AUG25 = pd.Timestamp("2026-08-25")
DEC15 = pd.Timestamp("2026-12-15")
AUG25_TARGET = 45_027_066
HEADWIND = REPO_ROOT / "data-official/2026-08/adjustments/headwind.json"
RUNS = HERE / "runs"
PARQUET = "mozaic_daily_forecast.2026-07-28.ld-D.adj-lo.parquet"

CURVES = [
    ("s01 LOCKED (canonical)",
     REPO_ROOT / "data-official/2026-08/desktop_locked" / PARQUET,
     "tab:blue", "-", 2.6),
    ("STAGED — cps.2049 rec14  ·  6.58% of gap  ·  kink 1.50×",
     RUNS / "cps0.2049_thresh032_recent14_cpr0.814_ncp40_clip0.6_sps0.00825_regimemultiplicative" / PARQUET,
     "tab:green", "-", 2.4),
    ("SPIKE — cps.1649 rec17  ·  9.52% of gap  ·  kink 1.73×",
     RUNS / "cps0.1649_thresh032_recent17_cpr0.814_ncp40_clip0.6_sps0.00825_regimemultiplicative" / PARQUET,
     "tab:purple", "-", 2.4),
]


def curve(parquet: Path) -> tuple[pd.Series, pd.Series]:
    """Post-headwind 28d-MA over the display window, plus the actuals MA."""
    df, _ = load_forecast(str(parquet))
    mask = (df["country"] == "ALL") & (df["segment"] == '{"os": "ALL"}')
    sub = df.loc[mask].copy()
    sub["target_date"] = pd.to_datetime(sub["target_date"])
    sub = sub.sort_values("target_date")

    ma = display_ma(sub["target_date"], sub["dau"], FORECAST_START)
    spec = json.loads(HEADWIND.read_text())
    start, anchor = pd.Timestamp(spec["start_date"]), pd.Timestamp(spec["anchor_date"])
    elapsed = pd.Series((ma.index - start).days, index=ma.index).clip(lower=0)
    ramp = spec["desktop_dau"] * elapsed / (anchor - start).days
    ma = ma + ramp.where(ma.index >= FORECAST_START, 0.0)

    actuals = sub.loc[sub["data_type"] == "training"].set_index("target_date")["dau"]
    return ma.loc[DISPLAY_START:DISPLAY_END], actuals.rolling(28).mean().loc[DISPLAY_START:]


def main() -> int:
    series, actuals = {}, None
    for label, parquet, colour, style, width in CURVES:
        ma, act = curve(parquet)
        series[label] = (ma, colour, style, width)
        if actuals is None:
            actuals = act

    july = pd.read_csv(
        REPO_ROOT / "data-official/2026-07/csv/july_canonical_curves.csv", parse_dates=["date"]
    ).set_index("date")["desktop_current_july"]

    fig, (full, zoom, dev) = plt.subplots(3, 1, figsize=(15, 16))
    locked = series["s01 LOCKED (canonical)"][0]

    for axis in (full, zoom):
        for label, (ma, colour, style, width) in series.items():
            axis.plot(ma.index, ma.values, color=colour, ls=style, lw=width, label=label)
        # Drawn last with a high zorder: pre-seam every model curve IS the actuals series, so a
        # black line underneath them is invisible and the legend entry reads as a missing curve.
        axis.plot(actuals.index, actuals.values, color="black", lw=2.4, zorder=8,
                  label="Actuals (28d MA)")
        axis.plot(july.index, july.values, color="tab:red", lw=1.9, ls="--",
                  label="July delivered")
        axis.plot([AUG25], [AUG25_TARGET], marker="*", ms=20, color="crimson", ls="none",
                  zorder=7, label=f"Aug-25 target {AUG25_TARGET:,.0f}")
        axis.axvline(FORECAST_START, color="grey", lw=1, ls=":")
        axis.grid(alpha=0.3)
        # Two decimals: the candidates sit ~0.13M apart on a ~45M curve, so whole-million tick
        # labels would render several identical "45M" ticks.
        axis.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e6:.2f}M"))

    full.set_xlim(DISPLAY_START, DISPLAY_END)
    full.set_title("August desktop DAU, 28-day MA, post-headwind — full year\n"
                   "The three model curves are indistinguishable at this scale; see below",
                   fontsize=13)
    full.legend(loc="lower right", fontsize=10)

    zoom.set_xlim(pd.Timestamp("2026-07-01"), pd.Timestamp("2026-10-15"))
    zoom.set_ylim(43_000_000, 48_000_000)
    zoom.axvline(AUG25, color="crimson", lw=1, ls=":")
    zoom.set_title("Near-horizon zoom (Jul–Oct) — where the entire difference lives", fontsize=13)
    zoom.legend(loc="lower right", fontsize=10)

    # -- (c) deviation from the locked build ---------------------------------------------
    for label, (ma, colour, style, width) in series.items():
        if "LOCKED" in label:
            continue
        dev.plot(ma.index, (ma - locked).values, color=colour, lw=2.4, label=label)
    # July is deliberately absent here: it sits ~1.96M below the locked build at Aug-25, so on a
    # +/-400K axis it would be off-scale except for a stray fragment near year-end, which reads as
    # a real crossing rather than as clipping. Panels (a) and (b) carry the July comparison.
    dev.axhline(0, color="tab:blue", lw=2.2, label="s01 LOCKED (baseline)")
    dev.axvline(AUG25, color="crimson", lw=1, ls=":")
    dev.axvline(DEC15, color="grey", lw=1, ls=":")
    dev.axvline(FORECAST_START, color="grey", lw=1, ls=":")
    dev.set_xlim(pd.Timestamp("2026-07-01"), DISPLAY_END)
    dev.set_ylim(-400_000, 200_000)
    dev.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e3:+,.0f}K"))
    dev.grid(alpha=0.3)
    dev.set_title("Deviation from the locked build — the only view that separates the candidates\n"
                  "(July omitted: −1,961,825 at Aug-25, far off this scale)", fontsize=13)
    dev.axhline(-196_183, color="crimson", ls="--", lw=1.6,
                label="the ask: −196,183 (10% of gap)")

    for label, (ma, colour, _, _) in series.items():
        if "LOCKED" in label:
            continue
        for date, tag in ((AUG25, "Aug-25"), (DEC15, "Dec-15")):
            value = float(ma.loc[date] - locked.loc[date])
            dev.annotate(f"{tag} {value:+,.0f}", xy=(date, value),
                         xytext=(8, -15 if colour == "tab:green" else 9),
                         textcoords="offset points", fontsize=9, color=colour, weight="bold")
    dev.legend(loc="lower left", fontsize=10)

    fig.tight_layout()
    out = HERE / "plots" / "candidates_vs_locked.png"
    fig.savefig(out, dpi=140, bbox_inches="tight")
    print(f"Wrote {out}\n")

    rows = [("July delivered", float(july.loc[AUG25]), float(july.loc[DEC15]))]
    for label, (ma, _, _, _) in series.items():
        rows.append((label, float(ma.loc[AUG25]), float(ma.loc[DEC15])))
    print(f"{'curve':<38}{'Aug-25':>14}{'Dec-15':>14}{'vs locked @Aug-25':>20}")
    for label, aug, dec in rows:
        print(f"{label:<38}{aug:>14,.0f}{dec:>14,.0f}"
              f"{aug - float(locked.loc[AUG25]):>20,.0f}")
    print(f"{'Aug-25 TARGET':<38}{AUG25_TARGET:>14,.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
