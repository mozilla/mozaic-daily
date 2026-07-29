#!/usr/bin/env python3
"""HTML report for the Aug-trough desktop search: parameter table + analytical
Win10-headwind delta (τ) + a 2D (Dec × Aug) reachability plot.

Covers rounds 1-3 (gradient) and the LHS `sampling/` round. The headwind is applied
analytically: anchor A(τ) = -1,370,000 + τ (base = June -1,420,000 + 50K real global
change; τ = the "how much Prophet already absorbed" belief, currently +25K), tunable
over [-200K, +200K]. Production applies the ramp (adjustments.render_adjustment ->
apply_net_adjustment_to_series) as A·(date-Apr1)/(Dec15-Apr1) on the 28d-MA; the scorer
reproduces the canonical to the dollar. So:
    Aug-22 display(τ) = aug_pre + A(τ)·f_aug      (f_aug ≈ 0.55427)
    Dec-15 display(τ) = dec_pre + A(τ)
Each config is a slope-f_aug line in (Dec, Aug) space as τ slides. τ* that holds Dec at
48.585M = 1,370,000 - (dec_pre - 48.585M); feasible iff τ* ∈ [-200K, +200K].

Output: research/param-scans/aug22-retune/parameter_table.html (self-contained).
"""

from __future__ import annotations

import base64
import io
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from mozaic_daily.adjustments import load_forecast  # noqa: E402
from score_near_horizon import _daily_series, _headwind_ramp, score_dataframe  # noqa: E402

RETUNE = REPO_ROOT / "research/param-scans/aug22-retune"
HEADWIND = json.loads((REPO_ROOT / "data-official/2026-07/adjustments/headwind.json").read_text())
T_DEC = 48_585_483          # Dec-15 target
DEC_TOL = 10_000
AUG_BULLSEYE = 45_060_000
AUG_TOL = 100_000
AUG = pd.Timestamp("2026-08-22")
DEC = pd.Timestamp("2026-12-15")
F_AUG = _headwind_ramp(AUG, HEADWIND) / HEADWIND["desktop_dau"]   # ramp fraction at Aug-22 ≈ 0.55427
BASE = -1_370_000           # headwind anchor base (before τ)
TAU_LO, TAU_HI = -200_000, 200_000
TAU_REAS = (-75_000, 125_000)   # "reasonable" τ band (~±100K around the current +25K)
TAU_CURRENT = 25_000            # current headwind = BASE + 25K = -1,345,000


def post_ma28(df: pd.DataFrame) -> pd.Series:
    """Post-headwind (current τ) Global 28d-MA over its full date range (for the thumbnail)."""
    s = _daily_series(df, "ALL").rolling(28).mean()
    ramp = pd.Series({d: _headwind_ramp(d, HEADWIND) for d in s.index})
    return (s + ramp).dropna()


def thumb(series: pd.Series) -> str:
    s = series[(series.index >= "2026-01-01") & (series.index <= "2026-12-31")]
    fig, ax = plt.subplots(figsize=(3.4, 1.1), dpi=80)
    ax.plot(s.index, s.values / 1e6, color="#1f77b4", lw=1.1)
    for d, c in [(AUG, "#d76757"), (DEC, "#7d7dbd")]:
        ax.axvline(d, color=c, lw=0.8, ls="--")
    ax.axhspan((AUG_BULLSEYE - AUG_TOL) / 1e6, (AUG_BULLSEYE + AUG_TOL) / 1e6, color="#d76757", alpha=0.12)
    ax.set_ylim(42, 51)
    ax.tick_params(labelsize=5, length=2)
    ax.set_xticks([pd.Timestamp("2026-01-01"), AUG, DEC])
    ax.set_xticklabels(["Jan", "Aug22", "Dec15"], fontsize=5)
    ax.margins(x=0.01)
    for sp in ("top", "right"):
        ax.spines[sp].set_visible(False)
    fig.tight_layout(pad=0.2)
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


def collect() -> list[dict]:
    seen, rows = set(), []
    paths = (sorted(RETUNE.glob("round*/*/*/mozaic_daily_forecast.*.ld-D.adj-*.parquet"))
             + sorted(RETUNE.glob("sampling/*/*/mozaic_daily_forecast.*.ld-D.adj-*.parquet")))
    for parquet in paths:
        cfg = json.loads((parquet.parent / "parameters.json").read_text())["config"]
        key = (cfg["seasonality_regime"], cfg["prophet_changepoint_prior_scale"],
               cfg["prophet_changepoint_range"], cfg["prophet_recent_weeks"],
               cfg["prophet_n_changepoints"], cfg["prophet_seasonality_prior_scale"])
        if key in seen:
            continue
        seen.add(key)
        df, _ = load_forecast(str(parquet))
        s = score_dataframe(df, "2026-08-22", HEADWIND)
        aug_pre, dec_pre = s["global_target_pre"], s["global_dec15_pre"]
        tau_star = 1_370_000 - (dec_pre - T_DEC)          # τ that puts Dec-15 at target
        aug_at_hold = aug_pre + F_AUG * (BASE + tau_star)  # Aug-22 when Dec held (if τ* feasible)
        rows.append({
            "regime": cfg["seasonality_regime"], "cps": cfg["prophet_changepoint_prior_scale"],
            "cpr": cfg["prophet_changepoint_range"], "recent": cfg["prophet_recent_weeks"],
            "ncp": cfg["prophet_n_changepoints"], "sps": cfg["prophet_seasonality_prior_scale"],
            "aug_pre": aug_pre, "dec_pre": dec_pre,
            "aug_post": s["global_target_post"], "dec_post": s["global_dec15_post"],
            "tau_star": tau_star, "feasible": abs(tau_star) <= 200_000,
            "aug_at_hold": aug_at_hold, "thumb": thumb(post_ma28(df)),
        })
    rows.sort(key=lambda r: (r["feasible"], r["aug_at_hold"]), reverse=True)
    return rows


def reachability_plot(rows: list[dict]) -> str:
    """2D (Dec × Aug): each config a τ-line over [-200K,+200K]; target box + reasonable-τ band."""
    fig, ax = plt.subplots(figsize=(7.2, 6.0), dpi=110)
    for r in rows:
        A0, A1 = BASE + TAU_LO, BASE + TAU_HI
        xs = [(r["dec_pre"] + A0) / 1e6, (r["dec_pre"] + A1) / 1e6]
        ys = [(r["aug_pre"] + F_AUG * A0) / 1e6, (r["aug_pre"] + F_AUG * A1) / 1e6]
        is_mult = r["regime"] == "multiplicative"
        ax.plot(xs, ys, color=("#1f77b4" if is_mult else "#bbbbbb"),
                lw=(1.0 if is_mult else 0.7), alpha=(0.55 if is_mult else 0.4), zorder=2 if is_mult else 1)
        # reasonable-τ sub-segment (thicker) for multiplicative configs
        if is_mult:
            Ar0, Ar1 = BASE + TAU_REAS[0], BASE + TAU_REAS[1]
            ax.plot([(r["dec_pre"] + Ar0) / 1e6, (r["dec_pre"] + Ar1) / 1e6],
                    [(r["aug_pre"] + F_AUG * Ar0) / 1e6, (r["aug_pre"] + F_AUG * Ar1) / 1e6],
                    color="#1f77b4", lw=2.4, alpha=0.9, zorder=3, solid_capstyle="round")
    # target box
    ax.add_patch(plt.Rectangle(((T_DEC - DEC_TOL) / 1e6, (AUG_BULLSEYE - AUG_TOL) / 1e6),
                               2 * DEC_TOL / 1e6, 2 * AUG_TOL / 1e6, fill=True,
                               color="#2ca02c", alpha=0.35, zorder=5, ec="#1a7a1a", lw=1.5))
    ax.axvline(T_DEC / 1e6, color="#7d7dbd", lw=0.8, ls=":")
    ax.axhline(AUG_BULLSEYE / 1e6, color="#d76757", lw=0.8, ls=":")
    ax.set_xlabel("Dec-15 28d-MA (display), M")
    ax.set_ylabel("Aug-22 28d-MA (display), M")
    ax.set_title("Reachability: each config swept over τ ∈ [−200K, +200K]\n"
                 "blue = multiplicative (sampled); thick = reasonable-τ band; grey = auto/amplitude ref; "
                 "green box = target", fontsize=9)
    ax.set_xlim(47.0, 50.5)
    ax.set_ylim(43.0, 47.5)
    ax.grid(alpha=0.15)
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


def m(v: float) -> str:
    return f"{v/1e6:.3f}M"


def build_html(rows: list[dict], reach_png: str) -> str:
    def aug_class(v):
        ad = abs(v - AUG_BULLSEYE)
        return "good" if ad <= AUG_TOL else ("near" if ad <= 300_000 else "bad")
    def tau_cell(r):
        cls = "good" if r["feasible"] and abs(r["tau_star"] - TAU_CURRENT) <= 100_000 else \
              ("near" if r["feasible"] else "bad")
        return f'<td class="{cls}">{r["tau_star"]:+,.0f}</td>'
    trs = []
    for r in rows:
        params = (f'{r["regime"]} · cps{r["cps"]} · cpr{r["cpr"]} · '
                  f'recent{r["recent"]} · ncp{r["ncp"]} · sps{r["sps"]}')
        trs.append(
            f'<tr><td class="p">{params}</td>'
            f'<td>{m(r["aug_post"])}</td><td>{m(r["dec_post"])}</td>'
            f'<td>{m(r["aug_pre"])}</td><td>{m(r["dec_pre"])}</td>'
            f'{tau_cell(r)}'
            f'<td class="{aug_class(r["aug_at_hold"])}">{m(r["aug_at_hold"])}</td>'
            f'<td class="{aug_class(r["aug_at_hold"])}">{r["aug_at_hold"]-AUG_BULLSEYE:+,.0f}</td>'
            f'<td><img src="data:image/png;base64,{r["thumb"]}"></td></tr>'
        )
    return f"""<!doctype html><html><head><meta charset="utf-8"><style>
body{{font:13px -apple-system,Helvetica,Arial;margin:24px;color:#222}}
h1{{font-size:19px}} .sub{{color:#555;max-width:1000px}}
table{{border-collapse:collapse;margin-top:16px}}
th,td{{border:1px solid #ddd;padding:5px 8px;text-align:right;white-space:nowrap}}
th{{background:#f4f4f4;font-size:11px;text-align:center}}
td.p{{text-align:left;font-family:ui-monospace,Menlo,monospace;font-size:11px}}
td.good{{background:#d7f0d7;font-weight:600}} td.near{{background:#fff3cd}} td.bad{{background:#f5d5d0}}
img{{display:block}} code{{background:#f0f0f0;padding:1px 4px;border-radius:3px}}
.plot{{margin:16px 0;max-width:760px}}
</style></head><body>
<h1>Aug-trough desktop parameter table + analytical headwind (τ) — anchor 2026-07-06</h1>
<p class="sub"><b>Target:</b> Aug-22 28d-MA (display) → <b>{m(AUG_BULLSEYE)} ±0.1M</b>;
<b>Dec-15</b> within 10k of <b>{m(T_DEC)}</b>. Headwind anchor <code>A(τ)=−1,370,000+τ</code>,
τ∈[−200K,+200K] (current τ=+25K ⇒ −1,345,000). Aug-22 gets fraction <code>{F_AUG:.4f}</code> of the
anchor, Dec-15 the full anchor — so sliding τ moves each config along a slope-{F_AUG:.3f} line.
<b>τ*</b> = the delta that pins Dec-15 at target (green if within ±100K of current & feasible; red if
|τ*|&gt;200K = can't hold Dec even at the extreme). <b>Aug@hold</b> = Aug-22 once τ=τ* (green = in the
±0.1M band). Sorted feasible-first, then Aug@hold.</p>
<div class="plot"><img src="data:image/png;base64,{reach_png}" style="width:100%"></div>
<table><thead><tr>
<th>parameters</th><th>Aug-22<br>(τ=+25K)</th><th>Dec-15<br>(τ=+25K)</th>
<th>Aug-22<br>(pre-hw)</th><th>Dec-15<br>(pre-hw)</th>
<th>τ* to hold<br>Dec-15</th><th>Aug@hold<br>(→45.06M)</th><th>dist to<br>target</th>
<th>display 28d-MA curve (2026)</th>
</tr></thead><tbody>{''.join(trs)}</tbody></table>
</body></html>"""


def main() -> int:
    rows = collect()
    reach = reachability_plot(rows)
    out = RETUNE / "parameter_table.html"
    out.write_text(build_html(rows, reach))
    n_mult = sum(r["regime"] == "multiplicative" for r in rows)
    best = max(rows, key=lambda r: r["aug_at_hold"] if r["feasible"] else -1e18)
    print(f"Wrote {out} ({len(rows)} configs, {n_mult} multiplicative)")
    print(f"Best feasible Aug@hold: {best['aug_at_hold']/1e6:.3f}M "
          f"(τ*={best['tau_star']:+,.0f}; cpr{best['cpr']} ncp{best['ncp']} cps{best['cps']}) "
          f"dist {best['aug_at_hold']-AUG_BULLSEYE:+,.0f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
