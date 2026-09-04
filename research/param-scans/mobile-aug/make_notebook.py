#!/usr/bin/env python3
"""Build `round1_gradient.ipynb` for the August mobile parameter search.

Covers BOTH regime runs: the shipped `seasonality_regime='auto'` center and the
`multiplicative` re-run of the same 11 probes. The notebook is generated rather than
hand-written so it can be regenerated after a re-scan without hand-editing JSON.

    source .venv/bin/activate
    python research/param-scans/mobile-aug/make_notebook.py
    jupyter nbconvert --to notebook --execute --inplace \\
        research/param-scans/mobile-aug/round1_gradient.ipynb

Cells are named `# [short-name]` per the repo convention.
"""

from pathlib import Path

import nbformat as nbf

HERE = Path(__file__).resolve().parent
OUT = HERE / "round1_gradient.ipynb"

CELLS: list[tuple[str, str]] = []


def code(source: str) -> None:
    CELLS.append(("code", source.strip("\n")))


def md(source: str) -> None:
    CELLS.append(("markdown", source.strip("\n")))


md("""
# August 2026 mobile — round-1 sensitivity gradient

Central-difference first and second derivatives of the **Dec-15 2026 28d-MA, post-headwind**
with respect to each numeric non-holiday knob, run twice: once at the shipped
`seasonality_regime='auto'` center and once with the regime forced to `multiplicative`.

**Target 17,923,869 ± 50,000** (July delivered). The shipped build reads 17,601,155 →
gap **+322,714**.

The gap is a calibration artifact of the 2026-07-31 `m` → `p` methodology swap, not a data
change. Closing it raises the fitted trend: in calendar-aligned terms the search must move
Dec-15 year-over-year growth from **12.35% → 14.41%** against a measured organic rate of
**11.60%**. `yoy_dec15_pct` is reported on every probe so that cost stays visible.

Scored on Dec-15 only. Seam handoff is **reported, never trained on**.

Context and scope decisions: `_index.md` in this directory.
""")

code("""
# [setup]
import importlib.util
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

REPO = Path.cwd()
while not (REPO / "src" / "mozaic_daily").exists() and REPO != REPO.parent:
    REPO = REPO.parent
sys.path.insert(0, str(REPO / "src"))

HERE = REPO / "research/param-scans/mobile-aug"
PLOTS = HERE / "plots"
PLOTS.mkdir(exist_ok=True)

_spec = importlib.util.spec_from_file_location(
    "mobile_scoring", REPO / "scripts" / "mobile_scoring.py")
mobile_scoring = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mobile_scoring)

from mozaic_daily.adjustments import load_forecast
from mozaic_daily.seam_ma import display_ma

FORECAST_START = pd.Timestamp(mobile_scoring.FORECAST_START)
DEC15 = pd.Timestamp(mobile_scoring.DEC15)
TARGET = mobile_scoring.TARGET_DEC15
TOLERANCE = mobile_scoring.TOLERANCE
HEADWIND = mobile_scoring.load_headwind(mobile_scoring.DEFAULT_HEADWIND)

# The three runs. 'auto' is the shipped regime; the other two re-ran the same 11 probes
# with the regime forced. See [reconciliation] for why 'additive' moves nothing at the total.
REGIMES = {"auto": "round1", "multiplicative": "round1_mult", "additive": "round1_addi"}

# Display window: actuals from Jan 1 through the end of the forecast, per the canonical
# plot format used across this repo's parameter sweeps.
DISPLAY_START = pd.Timestamp("2026-01-01")
DISPLAY_END = pd.Timestamp("2026-12-31")

print(f"repo   {REPO}")
print(f"seam   {FORECAST_START.date()}   target {TARGET:,} +-{TOLERANCE:,}")
print(f"headwind at Dec-15: {mobile_scoring.headwind_ramp(DEC15, HEADWIND):+,.0f}")
""")

code("""
# [load-scores]
scores, deriv, centers = {}, {}, {}
for regime, tag in REGIMES.items():
    s = pd.read_csv(HERE / f"{tag}_scores.csv")
    d = pd.read_csv(HERE / f"{tag}_derivatives.csv")
    # Curvature over the same +-10% span the effect column uses: 0.5 * d2 * (0.1*center)^2.
    # When this is a large fraction of the linear term, the extrapolation is untrustworthy
    # even for a single knob in isolation.
    d["curvature_over_span"] = 0.5 * d["d2"] * (0.10 * d["center"]) ** 2
    d["nonlinearity"] = (d["curvature_over_span"].abs()
                         / d["effect_per_10pct"].abs().replace(0, np.nan))
    scores[regime], deriv[regime] = s, d
    centers[regime] = s.loc[s["axis"] == "(center)"].iloc[0]

F0 = float(centers["auto"]["dec15_post"])          # the shipped build
GAP = TARGET - F0
PRIOR_MA = float(centers["auto"]["prior_dec15_ma"])
YOY_NEEDED = ((TARGET - mobile_scoring.headwind_ramp(DEC15, HEADWIND)) / PRIOR_MA - 1) * 100

print(f"Shipped center (auto)      : {F0:,.0f}")
print(f"Target                     : {TARGET:,.0f} +-{TOLERANCE:,}")
print(f"Gap to close               : {GAP:+,.0f}")
print(f"Center YoY at Dec-15       : {centers['auto']['yoy_dec15_pct']:.2f}%")
print(f"YoY required to hit target : {YOY_NEEDED:.2f}%")
print(f"Measured organic growth    : 11.60%")
""")

code("""
# [regime-comparison]
# The regime's OWN effect: center vs center, everything else identical. On mobile the regime
# sets seasonality_mode only -- growth stays volume-driven, unlike desktop, where forcing
# multiplicative was the largest single lever in the summer-trough search.
auto_c = centers["auto"]
regime_effect = {r: float(centers[r]["dec15_post"]) - float(auto_c["dec15_post"])
                for r in REGIMES}

rows = pd.DataFrame([
    {"regime": r, "dec15_post": float(centers[r]["dec15_post"]),
     "gap_to_target": float(centers[r]["gap_to_target"]),
     "yoy_dec15_pct": float(centers[r]["yoy_dec15_pct"]),
     "seam_step": float(centers[r]["seam_step"]),
     "seam_slope_kink": float(centers[r]["seam_slope_kink"])}
    for r in REGIMES])
display(rows.style.format({"dec15_post": "{:,.0f}", "gap_to_target": "{:+,.0f}",
                           "yoy_dec15_pct": "{:.2f}", "seam_step": "{:+,.0f}",
                           "seam_slope_kink": "{:+,.0f}"}))

for r, v in regime_effect.items():
    if r == "auto":
        continue
    print(f"Regime effect ({r} - auto): {v:+,.0f} DAU at Dec-15  "
          f"= {v / GAP * 100:.1f}% of the {GAP:+,.0f} gap")
print("\\n`additive` lands within a couple of DAU of `auto`. That is NOT a failed override --")
print("the config reaches the model and 63 of 64 leaf tiles move. See [reconciliation].")
""")

code("""
# [derivative-table]
# d1 = (f(+h) - f(-h)) / 2h            first derivative of Dec-15 wrt the knob
# d2 = (f(+h) - 2f(0) + f(-h)) / h^2   curvature -- how fast the linear estimate degrades
#
# Rank by `effect_per_10pct`, NOT d1: the knobs' units differ by three orders of magnitude
# (cps ~0.035 vs n_changepoints ~25), so raw slopes are not comparable across rows.
for regime in REGIMES:
    d = deriv[regime]
    ranked = d.reindex(d["effect_per_10pct"].abs().sort_values(ascending=False).index)
    print(f"\\n=== regime={regime}  (center {centers[regime]['dec15_post']:,.0f}) ===")
    display(ranked[["axis", "center", "delta", "f_minus", "f_center", "f_plus",
                    "d1", "d2", "effect_per_10pct", "curvature_over_span", "nonlinearity"]]
            .style.format({"f_minus": "{:,.0f}", "f_center": "{:,.0f}", "f_plus": "{:,.0f}",
                           "d1": "{:,.0f}", "d2": "{:,.3g}",
                           "effect_per_10pct": "{:+,.0f}",
                           "curvature_over_span": "{:+,.0f}", "nonlinearity": "{:.1%}"}))
    print(f"sum of all five 10% effects: {d['effect_per_10pct'].sum():+,.0f}  "
          f"(vs gap {GAP:+,.0f})")
    print(f"all-favourable 10% budget:   {d['effect_per_10pct'].abs().sum():+,.0f}  "
          f"= {d['effect_per_10pct'].abs().sum() / GAP * 100:.0f}% of the gap")
""")

code("""
# [load-curves]
# Rebuild each probe's post-headwind display 28d-MA so the gradient can be read as curves,
# not just endpoint numbers. Actuals come from a probe's training rows (identical across all
# probes -- one shared raw pull), which under `p` equal raw actuals exactly.
def probe_curve(slug):
    path = (HERE / "results" / slug
            / f"mozaic_daily_forecast.{FORECAST_START.date()}.gm-D.adj-p.parquet")
    df, _ = load_forecast(str(path), require_state=["p"])
    daily = mobile_scoring.mobile_daily_series(df)
    ma = display_ma(daily["target_date"], daily["dau"], FORECAST_START)
    ramp = pd.Series([mobile_scoring.headwind_ramp(d, HEADWIND) for d in ma.index],
                     index=ma.index)
    ramp[ma.index < FORECAST_START] = 0.0
    return ma + ramp, daily

curves, actuals_daily = {}, None
for regime in REGIMES:
    for _, row in scores[regime].iterrows():
        curves[(regime, row["label"])], daily = probe_curve(row["slug"])
        if actuals_daily is None:
            actuals_daily = daily

actual = actuals_daily[actuals_daily["data_type"] == "training"].set_index("target_date")["dau"]
actual_ma = actual.rolling(28).mean()

print(f"loaded {len(curves)} probe curves across {len(REGIMES)} regimes; "
      f"actuals through {actual.index.max().date()}")
""")

code("""
# [plot-helpers]
def millions(x, _pos):
    # Two decimals: the probes sit within ~0.3M of each other, so whole-million labels would
    # render several adjacent ticks identically.
    return f"{x / 1e6:.2f}M"

def base_axes(title):
    fig, ax = plt.subplots(figsize=(13, 6))
    ax.axhspan(TARGET - TOLERANCE, TARGET + TOLERANCE, color="tab:green", alpha=0.12,
               zorder=0, label=f"target band {TARGET:,.0f} +-{TOLERANCE:,}")
    ax.axhline(TARGET, color="tab:green", lw=1.2, ls="--", zorder=1)
    a = actual_ma[(actual_ma.index >= DISPLAY_START) & (actual_ma.index <= DISPLAY_END)].dropna()
    ax.plot(a.index, a.values, color="black", lw=2.0, label="actuals (28d-MA)", zorder=5)
    ax.axvline(FORECAST_START, color="grey", lw=1.0, ls=":", zorder=2)
    ax.set_title(title, fontsize=12)
    ax.set_ylabel("DAU (28-day MA)")
    ax.yaxis.set_major_formatter(plt.FuncFormatter(millions))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    ax.grid(alpha=0.25)
    return fig, ax

def draw(ax, regime, label, color, ls, text):
    curve = curves[(regime, label)]
    seg = curve[(curve.index >= FORECAST_START) & (curve.index <= DISPLAY_END)].dropna()
    ax.plot(seg.index, seg.values, color=color, ls=ls, lw=1.8, label=text, zorder=4)
    ax.plot([DEC15], [seg.loc[DEC15]], marker="o", color=color, ms=6, zorder=6)

def save(fig, name):
    out = PLOTS / name
    fig.tight_layout()
    fig.savefig(out, dpi=130)
    print(f"saved {out.relative_to(REPO)}")
    plt.show()
""")

code("""
# [plot-per-axis]
# One canonical-format chart per axis, per regime: -delta / center / +delta post-headwind
# 28d-MA against actuals from Jan 1, target band shaded. This is the per-parameter validation
# plot the Dec-15 table cannot replace -- two configs can share a Dec-15 and disagree all year.
STYLES = {-1: ("tab:blue", "-"), 0: ("tab:red", "-"), 1: ("tab:orange", "-")}

for regime in REGIMES:
    s = scores[regime]
    for axis in deriv[regime]["axis"]:
        rows = pd.concat([s[s["axis"] == axis], s[s["axis"] == "(center)"]])
        fig, ax = base_axes(f"August mobile round-1 — {axis}  [regime={regime}]\\n"
                            f"ALL MOBILE world DAU, 28d-MA, post-headwind")
        for _, row in rows.iterrows():
            color, ls = STYLES[int(row["sign"])]
            value = "center" if row["sign"] == 0 else f"{row['value']}"
            draw(ax, regime, row["label"], color, ls,
                 f"{value}  ->  Dec-15 {row['dec15_post']:,.0f}")
        ax.legend(loc="upper left", fontsize=9)
        suffix = "" if regime == "auto" else f"_{regime}"
        save(fig, f"round1_{axis.replace('-', '_')}{suffix}.png")
""")

code("""
# [plot-regime]
# The regime question in one frame: both centers plus each regime's best probe (cpr at -delta,
# the only knob with meaningful pull). If forcing multiplicative were the missing lever, the
# orange pair would sit visibly closer to the green band.
fig, ax = base_axes("August mobile — does forcing seasonality_regime help?\\n"
                    "ALL MOBILE world DAU, 28d-MA, post-headwind")
plan = [("auto", "center", "tab:red", "-"),
        ("auto", "changepoint-range__minus", "tab:red", "--"),
        ("multiplicative", "center", "tab:orange", "-"),
        ("multiplicative", "changepoint-range__minus", "tab:orange", "--"),
        ("additive", "center", "tab:purple", "-"),
        ("additive", "changepoint-range__minus", "tab:purple", "--")]
for regime, label, color, ls in plan:
    row = scores[regime].loc[scores[regime]["label"] == label].iloc[0]
    name = "center" if label == "center" else "cpr=0.725"
    draw(ax, regime, label, color, ls,
         f"{regime} / {name}  ->  {row['dec15_post']:,.0f}")
ax.legend(loc="upper left", fontsize=9)
save(fig, "round1_regime_comparison.png")
""")

code("""
# [plot-tornado]
# Knob sensitivity on a common scale: DAU moved at Dec-15 by a +10% change in each knob, both
# regimes side by side. The dashed line is the gap. Every bar is a sliver against it -- that
# single visual is the round-1 result.
order = list(deriv["auto"].reindex(
    deriv["auto"]["effect_per_10pct"].abs().sort_values().index)["axis"])
width, y = 0.38, np.arange(len(order))

fig, ax = plt.subplots(figsize=(11, 5))
for offset, (regime, color) in zip((-width / 2, width / 2),
                                   [("auto", "tab:blue"), ("multiplicative", "tab:orange")]):
    d = deriv[regime].set_index("axis").reindex(order)
    ax.barh(y + offset, d["effect_per_10pct"], height=width, color=color,
            alpha=0.85, label=f"regime={regime}")
    for yy, v in zip(y + offset, d["effect_per_10pct"]):
        ax.text(v, yy, f" {v:+,.0f}", va="center",
                ha="left" if v > 0 else "right", fontsize=8)

ax.axvline(0, color="black", lw=1.0)
ax.axvline(GAP, color="tab:green", lw=1.6, ls="--", label=f"gap to close {GAP:+,.0f}")
ax.set_yticks(y)
ax.set_yticklabels(order)
ax.set_xlabel("Dec-15 DAU moved by a +10% change in the knob")
ax.set_title("August mobile round-1 — knob sensitivity, both regimes")
ax.grid(axis="x", alpha=0.25)
ax.legend(loc="lower right", fontsize=9)
save(fig, "round1_tornado.png")
""")

code("""
# [reconciliation]
# WHY `additive` MOVES NOTHING -- and it is not a failed override.
#
# mozaic reconciles TOP-DOWN (`mozaic/utils.py`: metric_mozaics[m].reconcile_top_down()). The
# metric-level Mozaic forecasts the AGGREGATE series, then rescales the 64 country x app leaf
# tiles to sum to it. So a leaf tile's model form changes the ALLOCATION across countries; the
# world total is whatever the top-level fit says.
#
# mozaic's mobile model is volume-gated: under `auto` a tile is multiplicative iff max <= 2e6.
# The aggregate is ~16M, so the top-level fit is ALREADY additive under `auto`.
#   forcing additive       -> top-level fit unchanged        -> total unchanged
#   forcing multiplicative -> top-level fit flips            -> total moves
#
# The test below proves it: count how many leaves move, and compare the sum of their absolute
# moves against the change in the world total.
RESULTS = HERE / "results"
BASE = "cps0.035_thresh055_recent13_cpr0.75_ncp25_clip0.6_sps0.1"
SLUGS = {"auto": BASE, "multiplicative": BASE + "_regimemultiplicative",
         "additive": BASE + "_regimeadditive"}

snap = {}
for regime, slug in SLUGS.items():
    df, _ = load_forecast(
        str(RESULTS / slug / f"mozaic_daily_forecast.{FORECAST_START.date()}.gm-D.adj-p.parquet"),
        require_state=["p"])
    df["target_date"] = pd.to_datetime(df["target_date"])
    snap[regime] = df[df["target_date"] == DEC15]

rows = []
a = snap["auto"]
for regime in ("additive", "multiplicative"):
    m = a.merge(snap[regime], on=["country", "app_name", "segment"], suffixes=("_a", "_b"))
    leaf = m[(m["country"] != "ALL") & (m["app_name"] != "ALL MOBILE")]
    top = m[(m["country"] == "ALL") & (m["app_name"] == "ALL MOBILE")]
    delta = leaf["dau_b"] - leaf["dau_a"]
    rows.append({"regime": regime, "leaves_moved": int((delta.abs() > 1).sum()),
                 "leaves_total": len(leaf), "sum_abs_leaf_move": delta.abs().sum(),
                 "net_leaf_move": delta.sum(),
                 "world_total_move": float(top["dau_b"].iloc[0] - top["dau_a"].iloc[0])})
recon = pd.DataFrame(rows)
display(recon.style.format({"sum_abs_leaf_move": "{:,.0f}", "net_leaf_move": "{:+,.0f}",
                            "world_total_move": "{:+,.0f}"}))

print("Read the additive row: 63 of 64 leaves move, 27,552 DAU of absolute change, and the")
print("world total moves +1. The reallocation cancels because the total is pinned by the")
print("top-level fit. The knob is working; it just cannot reach this KPI.")
print()
print("CONSEQUENCE FOR THE SEARCH: the mobile world headline is, to first order, a SINGLE")
print("Prophet fit on the aggregate organic series. The 64 leaf tiles set allocation only.")
print("Any parameter whose effect varies across tiles will largely cancel at the total --")
print("only its effect on the top-level fit survives.")
""")

code("""
# [envelope]
# The reachable envelope across BOTH runs: every probe built, best first. This is the honest
# answer to "how close can these knobs get" -- it is a measured maximum over 22 configs, not
# an extrapolation.
allp = pd.concat([scores[r].assign(regime=r) for r in REGIMES], ignore_index=True)
allp["short"] = allp["regime"] + " / " + allp["label"]
best = allp.sort_values("dec15_post", ascending=False)

display(best[["short", "dec15_post", "gap_to_target", "yoy_dec15_pct",
              "seam_step", "seam_slope_kink"]].head(10)
        .style.format({"dec15_post": "{:,.0f}", "gap_to_target": "{:+,.0f}",
                       "yoy_dec15_pct": "{:.2f}", "seam_step": "{:+,.0f}",
                       "seam_slope_kink": "{:+,.0f}"}))

top = best.iloc[0]
print(f"\\nBest of {len(allp)} probes: {top['short']} = {top['dec15_post']:,.0f}")
print(f"  still short of target by {TARGET - top['dec15_post']:,.0f}")
print(f"  full envelope: {allp['dec15_post'].min():,.0f} to {allp['dec15_post'].max():,.0f} "
      f"(spread {allp['dec15_post'].max() - allp['dec15_post'].min():,.0f})")
print(f"  target sits {TARGET - allp['dec15_post'].max():,.0f} above the top of it")
print(f"\\nYoY across all probes: {allp['yoy_dec15_pct'].min():.2f}% to "
      f"{allp['yoy_dec15_pct'].max():.2f}%   (needed {YOY_NEEDED:.2f}%, "
      f"measured organic 11.60%)")
""")

code("""
# [seam-report]
# REPORTED, NOT TRAINED ON. Per the search brief, nothing here enters selection; it exists so
# a config that hits Dec-15 by mangling the actuals->forecast handoff is visible.
#
# `seam_step` is the level discontinuity (forecast 28d-MA on the first forecast day minus the
# actuals' own trailing 28d-MA on the last training day). Under `p` a small step is expected
# by construction: training rows carry MEASURED paid, forecast rows carry MARKETING's paid
# level, and the two disagree at the seam.
seam = allp[["short", "value", "seam_actual_ma", "seam_forecast_ma", "seam_step",
             "seam_step_pct", "seam_slope_before", "seam_slope_after", "seam_slope_kink"]]
display(seam.style.format({
    "seam_actual_ma": "{:,.0f}", "seam_forecast_ma": "{:,.0f}", "seam_step": "{:+,.0f}",
    "seam_step_pct": "{:+.3f}", "seam_slope_before": "{:+,.0f}",
    "seam_slope_after": "{:+,.0f}", "seam_slope_kink": "{:+,.0f}"}))

print(f"\\nseam step across all probes: {allp['seam_step'].min():+,.0f} to "
      f"{allp['seam_step'].max():+,.0f}")
print(f"slope kink across all probes: {allp['seam_slope_kink'].min():+,.0f} to "
      f"{allp['seam_slope_kink'].max():+,.0f}")
print("\\nNote the direction that helps Dec-15 (lower cpr) also IMPROVES the handoff, and that "
      "forcing multiplicative slightly worsens it.")
""")

code("""
# [extrapolation]
# Single-knob linear extrapolations to close the whole gap. These are ROUND-2 STARTING POINTS,
# NOT PREDICTIONS. Two independent reasons they will be wrong:
#   1. own-curvature -- `nonlinearity` says how much the quadratic term contributes over just a
#      10% move; extrapolating far past that compounds it.
#   2. cross-parameter interaction -- every prior search on this codebase found that combining
#      two knobs does not equal the sum of their individual effects. A +-delta gradient cannot
#      see this at all; only a combined cell can.
BOUNDS = {"changepoint-range": (0.0, 1.0), "n-changepoints": (1, 200),
          "recent-weeks": (2, 104), "changepoint-prior-scale": (0.001, 1.0),
          "seasonality-prior-scale": (0.001, 20.0)}

for regime in REGIMES:
    d = deriv[regime]
    gap_here = TARGET - float(centers[regime]["dec15_post"])
    print(f"\\n=== regime={regime}   gap {gap_here:+,.0f} ===")
    print(f"{'axis':<28}{'center':>10}{'linear target':>16}{'% move':>10}"
          f"{'reachable?':>14}{'nonlin':>9}")
    ranked = d.reindex(d["effect_per_10pct"].abs().sort_values(ascending=False).index)
    for _, r in ranked.iterrows():
        # Recompute against THIS regime's own center rather than reusing the auto gap.
        target_value = r["center"] + gap_here / r["d1"] if r["d1"] else float("nan")
        pct = (target_value - r["center"]) / r["center"] * 100
        lo, hi = BOUNDS.get(r["axis"], (-np.inf, np.inf))
        ok = "yes" if lo <= target_value <= hi else "OUT OF RANGE"
        print(f"{r['axis']:<28}{r['center']:>10}{target_value:>16,.4f}{pct:>+9.1f}%"
              f"{ok:>14}{r['nonlinearity']:>9.1%}")
""")

code("""
# [cpr-meaning]
# What a large changepoint_range move actually means. cpr is the FRACTION of the training
# window in which Prophet may place changepoints, so lowering it freezes the trend estimate
# further into the past -- and the extrapolated single-knob target lands in 2022.
train_start, train_end = actual.index.min(), actual.index.max()
span = (train_end - train_start).days
print(f"mobile training window: {train_start.date()} -> {train_end.date()}  ({span} days)\\n")
print(f"{'cpr':>8}{'last changepoint':>20}{'history left unhinged':>24}")
for cpr, note in [(0.82, "package default"), (0.775, "probe +d"), (0.75, "SHIPPED"),
                  (0.725, "probe -d"), (0.60, ""), (0.50, ""), (0.3568, "extrapolated target")]:
    last = train_start + pd.Timedelta(days=round(span * cpr))
    print(f"{cpr:>8}{str(last.date()):>20}{span - round(span * cpr):>19,} d   {note}")
print("\\nAt the extrapolated value the trend is pinned to the 2021-2022 regime and the last")
print("~3.6 years cannot bend it. That is plausibly WHY it lifts Dec-15 -- it recovers the")
print("steeper pre-paid-era slope, which is the same signal `p` was adopted to remove.")
""")

md("""
## Conclusion

**No combination of the exposed non-holiday knobs reaches 17,923,869 from this center at
defensible magnitudes.**

- Across all 22 probes the Dec-15 envelope is ~17.58M–17.65M. The target sits **~275K above
  the top of it**.
- `changepoint_range` is the only knob with real pull, and it points **down**. Its
  single-knob extrapolation (≈0.36) would confine every changepoint to before end-2022 —
  see `[cpr-meaning]`. Its curvature over that distance exceeds its linear term, so the
  extrapolation is not even a reliable estimate of what that config would produce.
- **Forcing `seasonality_regime='multiplicative'` is a dud on mobile**: +17,542 at Dec-15,
  5.4% of the gap, and it slightly worsens the seam handoff. This is the opposite of desktop,
  where the same switch was the largest single lever — because on mobile the regime sets
  `seasonality_mode` only, while on desktop it also flips linear/logistic growth.
- YoY across every probe spans 12.24–12.64%, against **14.41%** required and **11.60%**
  measured organic. The whole reachable neighbourhood is ~1.8pp short.

## Reading this

- **Rank by `effect_per_10pct`, not `d1`.** Units differ by three orders of magnitude.
- **`nonlinearity` is the trust metric for the extrapolation** — the quadratic term's share
  of the linear term over a 10% move.
- **A ± gradient cannot see cross-parameter interaction**, which every prior search on this
  codebase found material. Only a combined cell can.
- **Seam numbers are reported only.** Nothing in the selection rule touches them.
- **`seasonality_corr_threshold` is not available on mobile** — `MobileModelConfig` raises on
  any non-zero value, because mobile's regime switch is volume-driven rather than
  correlation-driven. Desktop-only; do not re-propose it.
- **Holiday knobs stay excluded** by standing policy.
""")


def main() -> None:
    nb = nbf.v4.new_notebook()
    nb.cells = [nbf.v4.new_code_cell(src) if kind == "code" else nbf.v4.new_markdown_cell(src)
                for kind, src in CELLS]
    nb.metadata = {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python"},
    }
    nbf.write(nb, OUT)
    print(f"wrote {OUT} ({len(nb.cells)} cells)")


if __name__ == "__main__":
    main()
