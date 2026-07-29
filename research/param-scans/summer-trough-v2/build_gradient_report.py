#!/usr/bin/env python3
"""Sensitivity report for the central-difference gradient at July's s01 center.

Computes, for each numeric parameter, the two-sided local slope of three outcomes:

    Aug-25 trough minimum (the objective), Dec-15 (the constraint),
    and the seam handoff kink (reported, not scored)

plus each knob's trough:Dec ratio.

**A knob that moves Dec-15 is not disqualified** -- a second knob can cancel that
drift. Two knobs combine to gain trough at zero net Dec-15 exactly when their
direction vectors are non-parallel, i.e. when their ratios differ. So the
feasibility test is the *spread* of ratios across knobs, not whether any single
one is favourable: uniformly unfavourable ratios are survivable, uniformly
parallel ones are not. ``compensation_pairs`` enumerates every pair accordingly.

Slopes are reported four ways because each answers a different question:
  - per +-delta step  -- what the probe actually measured
  - per unit          -- the raw derivative, dimensioned per parameter unit
  - per +10% of center -- the fair cross-knob ranking. Delta sizes differ per knob
                         (integer knobs bottom out at +-1, so the holiday radii are
                         20-33% of center), and the raw per-step column therefore
                         flatters the coarsely-stepped ones. Ranking uses this.
  - curvature         -- f(x+h) + f(x-h) - 2f(x), the second difference. Large
                         values mean the two-sided slope is hiding curvature and
                         the local-linear reading should not be trusted.

Usage
-----
    source .venv/bin/activate
    python research/param-scans/summer-trough-v2/build_gradient_report.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from mozaic_daily.adjustments import load_forecast  # noqa: E402
from score_near_horizon import score_dataframe  # noqa: E402
from run_s01_gradient import (  # noqa: E402
    CENTER, CORR_EQUIV, GRADIENT_AXES, DEFAULT_RESULTS_DIR, FORECAST_START,
    build_probe_list, resolve_slug,
)

HERE = REPO_ROOT / "research/param-scans/summer-trough-v2"
OUT_HTML = HERE / "s01_gradient_report.html"
OUT_CSV = HERE / "s01_gradient_slopes.csv"
PARQUET_NAME = f"mozaic_daily_forecast.{FORECAST_START}.ld-D.adj-lo.parquet"
HEADWIND = json.loads(
    (REPO_ROOT / "data-official/2026-08/adjustments/headwind.json").read_text())

DEC_CANONICAL = 48_672_970
DEC_TOL = 50_000
TROUGH_CANONICAL = 43_833_674
AUG15, AUG25 = "2026-08-15", "2026-08-25"

# Human labels + the delta each axis was probed at.
AXIS_META = {flag: (center, delta, kind) for flag, center, delta, kind in GRADIENT_AXES}
AXIS_META["seasonality-corr-threshold"] = (CORR_EQUIV, 0.05, "float")
NICE = {
    "changepoint-prior-scale": "changepoint_prior_scale",
    "changepoint-range": "changepoint_range",
    "recent-weeks": "recent_weeks",
    "n-changepoints": "n_changepoints",
    "seasonality-prior-scale": "seasonality_prior_scale",
    "holiday-threshold": "holiday_threshold",
    "holiday-max-radius": "holiday_max_radius",
    "holiday-min-radius": "holiday_min_radius",
    "holiday-effect-floor": "holiday_effect_floor",
    "seasonality-corr-threshold": "seasonality_corr_threshold",
}


def score_dir(build_dir: Path) -> dict | None:
    parquet = build_dir / PARQUET_NAME
    if not parquet.exists():
        return None
    df, _ = load_forecast(str(parquet))
    at25 = score_dataframe(df, target_date=AUG25, headwind_spec=HEADWIND)
    at15 = score_dataframe(df, target_date=AUG15, headwind_spec=HEADWIND)
    return {
        "aug15": at15["global_target_post"],
        "aug25": at25["global_target_post"],
        "trough": at25["trough_min_post"],
        "trough_date": at25["trough_min_date"],
        "dec15": at25["global_dec15_post"],
        "seam_kink": at25["seam_slope_kink_model"],
    }


def collect(results_dir: Path) -> dict[str, dict]:
    """label -> scored outcome, for every probe that produced a forecast."""
    scored = {}
    for probe in build_probe_list():
        slug = resolve_slug(probe["overrides"], results_dir)
        row = score_dir(results_dir / slug)
        if row is None:
            print(f"  WARNING: missing {probe['label']} ({slug})")
            continue
        row["slug"] = slug
        scored[probe["label"]] = row
    return scored


def gradient_table(scored: dict[str, dict]) -> pd.DataFrame:
    rows = []
    for flag in NICE:
        lo = scored.get(f"{flag}__minus")
        hi = scored.get(f"{flag}__plus")
        # corr_threshold's pair sits on the auto center; everything else on mult.
        base_label = ("center_auto_corr" if flag == "seasonality-corr-threshold"
                      else "center_mult")
        base = scored.get(base_label)
        if not (lo and hi and base):
            continue
        center, delta, kind = AXIS_META[flag]

        rec = {"parameter": NICE[flag], "center": center, "delta": delta,
               "kind": kind, "base_label": base_label}
        for key, out in [("trough", "trough"), ("dec15", "dec15"),
                         ("seam_kink", "seam_kink"), ("aug15", "aug15")]:
            step = (hi[out] - lo[out]) / 2.0            # per +-delta step
            rec[f"{key}_step"] = step
            rec[f"{key}_per_unit"] = step / delta
            # Elasticity-style: effect of moving the knob 10% of its center value.
            # Required for a fair ranking -- delta sizes differ per knob (integer
            # knobs bottom out at +-1, i.e. 20-33% of center for the radii), so the
            # raw per-step column systematically flatters the coarsely-stepped ones.
            frac = abs(delta / center) if center else float("nan")
            rec[f"{key}_per_10pct"] = step * (0.10 / frac) if frac else float("nan")
            rec[f"{key}_curv"] = hi[out] + lo[out] - 2 * base[out]
        rec["delta_pct_of_center"] = 100 * abs(delta / center) if center else float("nan")
        # The ratio that decides usefulness under a fixed Dec-15 budget.
        rec["gain_per_dec"] = (abs(rec["trough_step"]) / abs(rec["dec15_step"])
                               if abs(rec["dec15_step"]) > 1e-9 else float("inf"))
        rec["dec_moves_same_way"] = (rec["trough_step"] * rec["dec15_step"]) > 0
        rows.append(rec)

    df = pd.DataFrame(rows)
    return df.sort_values("trough_step", key=lambda s: s.abs(), ascending=False)


def fig_tornado(df: pd.DataFrame, col: str, title: str, unit: str) -> go.Figure:
    d = df.sort_values(f"{col}_step", key=lambda s: s.abs())
    colors = ["#c62828" if v < 0 else "#1565c0" for v in d[f"{col}_step"]]
    fig = go.Figure(go.Bar(
        x=d[f"{col}_step"], y=d["parameter"], orientation="h",
        marker=dict(color=colors),
        text=[f"{v:+,.0f}" for v in d[f"{col}_step"]], textposition="outside",
        hovertext=[f"{r['parameter']}<br>center {r['center']} ± {r['delta']}<br>"
                   f"per ±δ step: {r[f'{col}_step']:+,.0f} {unit}<br>"
                   f"per unit: {r[f'{col}_per_unit']:+,.0f} {unit}<br>"
                   f"curvature: {r[f'{col}_curv']:+,.0f}"
                   for _, r in d.iterrows()],
        hoverinfo="text",
    ))
    fig.add_vline(x=0, line=dict(color="#333", width=1))
    fig.update_layout(
        title=title, xaxis_title=f"change per ±δ step ({unit})",
        height=480, template="plotly_white", margin=dict(l=200, r=90, t=70, b=50),
        showlegend=False,
    )
    return fig


MAX_PLAUSIBLE_STEPS = 5.0  # beyond this, linear extrapolation is not credible


def compensation_pairs(df: pd.DataFrame) -> pd.DataFrame:
    """Trough gain achievable by each ordered knob pair at ZERO net Dec-15 drift.

    A single knob that moves Dec-15 is not disqualified -- a second knob can cancel
    that drift. Taking one +delta step of the *driver* A and
    ``steps_B = -dDec_A / dDec_B`` steps of the *compensator* B leaves

        net Dec-15 = 0
        net trough = (dT_A * dDec_B - dT_B * dDec_A) / dDec_B

    which is nonzero exactly when the two direction vectors are not parallel, i.e.
    when their trough:Dec ratios differ. So the question is never "does this knob
    move Dec-15" but "is there *spread* in the ratios". Uniformly unfavourable
    ratios are survivable; uniformly *parallel* ones are not.

    Caveat carried into the report: this is a first-order superposition estimate.
    The grid already proved this space is not globally linear (outcomes were
    bimodal), so every pair here is a hypothesis needing one confirming probe --
    and ``steps`` far above 1 compounds that risk, since it walks the compensator
    well outside the interval its slope was measured on.
    """
    rows = []
    for _, a in df.iterrows():
        for _, b in df.iterrows():
            if a["parameter"] == b["parameter"]:
                continue
            if abs(b["dec15_step"]) < 1e-9:
                continue  # cannot compensate with a knob that does not move Dec-15
            steps_b = -a["dec15_step"] / b["dec15_step"]
            net_trough = a["trough_step"] + steps_b * b["trough_step"]
            rows.append({
                "driver": a["parameter"],
                "compensator": b["parameter"],
                "steps_compensator": steps_b,
                "net_trough_gain": net_trough,
                "driver_trough": a["trough_step"],
                "driver_dec": a["dec15_step"],
                "comp_trough": b["trough_step"],
                "comp_dec": b["dec15_step"],
                "plausible": abs(steps_b) <= MAX_PLAUSIBLE_STEPS,
            })
    out = pd.DataFrame(rows)
    return out.sort_values("net_trough_gain", ascending=False).reset_index(drop=True)


def fig_tradeoff(df: pd.DataFrame) -> go.Figure:
    """Trough slope vs Dec-15 slope. What matters is angular SPREAD, not position.

    Rays from the origin are iso-ratio directions: two knobs on the same ray are
    parallel and cannot be combined to decouple the dates, however large their
    slopes. Two knobs on different rays can.
    """
    fig = go.Figure()
    lim = max(df["trough_step"].abs().max(), df["dec15_step"].abs().max()) * 1.3

    # Each knob's own direction, extended through the origin: knobs sharing a line
    # are parallel (no decoupling available between them).
    for _, r in df.iterrows():
        norm = max(abs(r["dec15_step"]), abs(r["trough_step"]), 1e-9)
        dx, dy = r["dec15_step"] / norm, r["trough_step"] / norm
        fig.add_trace(go.Scatter(
            x=[-dx * lim, dx * lim], y=[-dy * lim, dy * lim], mode="lines",
            line=dict(color="#ddd", width=1), showlegend=False, hoverinfo="skip"))

    fig.add_trace(go.Scatter(
        x=df["dec15_step"], y=df["trough_step"], mode="markers+text",
        text=df["parameter"], textposition="top center", textfont=dict(size=10),
        marker=dict(size=13, color=df["gain_per_dec"], colorscale="Viridis",
                    showscale=True, line=dict(width=1, color="#333"),
                    colorbar=dict(title="trough:Dec<br>ratio")),
        hovertext=[f"{r['parameter']}<br>trough {r['trough_step']:+,.0f}<br>"
                   f"Dec-15 {r['dec15_step']:+,.0f}<br>"
                   f"ratio {r['gain_per_dec']:.2f}" for _, r in df.iterrows()],
        hoverinfo="text", showlegend=False,
    ))
    fig.add_hline(y=0, line=dict(color="#333", width=1))
    fig.add_vline(x=0, line=dict(color="#333", width=1))
    fig.update_layout(
        title="Trough vs Dec-15 sensitivity — grey rays are each knob's direction. "
              "Knobs sharing a ray are parallel and cannot decouple; spread is what buys.",
        xaxis_title="Dec-15 change per ±δ step (DAU)",
        yaxis_title="Trough change per ±δ step (DAU)",
        height=580, template="plotly_white",
    )
    return fig


def fig_pairs(pairs: pd.DataFrame, top: int = 14) -> go.Figure:
    """Best compensating pairs: trough gain at zero net Dec-15 drift."""
    d = pairs[pairs["plausible"]].head(top).iloc[::-1]
    if d.empty:
        d = pairs.head(top).iloc[::-1]
    labels = [f"{r['driver']}  ←  {r['compensator']} ×{r['steps_compensator']:+.2f}"
              for _, r in d.iterrows()]
    fig = go.Figure(go.Bar(
        x=d["net_trough_gain"], y=labels, orientation="h",
        marker=dict(color=["#2e7d32" if v > 0 else "#c62828"
                           for v in d["net_trough_gain"]]),
        text=[f"{v:+,.0f}" for v in d["net_trough_gain"]], textposition="outside",
        hovertext=[f"driver <b>{r['driver']}</b> "
                   f"({r['driver_trough']:+,.0f} trough, {r['driver_dec']:+,.0f} Dec)<br>"
                   f"compensator <b>{r['compensator']}</b> × {r['steps_compensator']:+.3f} "
                   f"steps<br>net trough {r['net_trough_gain']:+,.0f} at Dec-15 drift 0"
                   for _, r in d.iterrows()],
        hoverinfo="text",
    ))
    fig.add_vline(x=0, line=dict(color="#333", width=1))
    fig.update_layout(
        title="Two-knob combinations — predicted trough gain at ZERO net Dec-15 drift "
              "(first-order; each needs a confirming probe)",
        xaxis_title="net trough gain per driver step (DAU)",
        height=520, template="plotly_white", margin=dict(l=420, r=90, t=70, b=50),
        showlegend=False,
    )
    return fig


def build_table(df: pd.DataFrame) -> str:
    head = ["parameter", "center", "±δ", "δ as % of center", "trough /step",
            "Dec-15 /step", "trough per +10%", "Dec per +10%", "trough:Dec ratio",
            "same sign", "seam kink /step", "trough curvature", "Dec curvature"]
    rows = []
    for _, r in df.iterrows():
        ratio = ("∞" if r["gain_per_dec"] == float("inf")
                 else f"{r['gain_per_dec']:.2f}")
        # Neutral: same-sign is not disqualifying, it just sets which knobs pair well.
        same = "yes" if r["dec_moves_same_way"] else "no"
        coarse = " class='bad'" if r["delta_pct_of_center"] > 15 else " class='dim'"
        rows.append(
            "<tr>"
            f"<td class='name'>{r['parameter']}</td>"
            f"<td class='dim'>{r['center']}</td><td class='dim'>±{r['delta']}</td>"
            f"<td{coarse}>{r['delta_pct_of_center']:.1f}%</td>"
            f"<td>{r['trough_step']:+,.0f}</td>"
            f"<td>{r['dec15_step']:+,.0f}</td>"
            f"<td><b>{r['trough_per_10pct']:+,.0f}</b></td>"
            f"<td>{r['dec15_per_10pct']:+,.0f}</td>"
            f"<td>{ratio}</td><td>{same}</td>"
            f"<td>{r['seam_kink_step']:+,.0f}</td>"
            f"<td class='dim'>{r['trough_curv']:+,.0f}</td>"
            f"<td class='dim'>{r['dec15_curv']:+,.0f}</td>"
            "</tr>")
    return ("<table><thead><tr>" + "".join(f"<th>{h}</th>" for h in head)
            + "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>")


def main() -> int:
    results_dir = DEFAULT_RESULTS_DIR
    print("Scoring gradient probes...")
    scored = collect(results_dir)
    print(f"  scored {len(scored)} probes")

    base = scored.get("center_mult")
    base_auto = scored.get("center_auto_corr")
    if base is None:
        raise SystemExit("center_mult missing -- has the gradient run?")

    df = gradient_table(scored)
    # Rank by the normalised slope, not the raw step -- see the note in gradient_table.
    df = df.sort_values("trough_per_10pct", key=lambda c: c.abs(), ascending=False)
    if df.empty:
        raise SystemExit("no complete +/- pairs yet")
    df.to_csv(OUT_CSV, index=False)

    # Center-form equivalence: substituting regime=auto+corr for forced multiplicative
    # is only legitimate if the two agree here.
    if base_auto:
        equiv = (f"trough {base_auto['trough'] - base['trough']:+,.0f}, "
                 f"Dec-15 {base_auto['dec15'] - base['dec15']:+,.0f}")
        ok = (abs(base_auto["trough"] - base["trough"]) < 5_000
              and abs(base_auto["dec15"] - base["dec15"]) < 5_000)
        equiv_box = (
            f"<p class='verdict {'good-box' if ok else 'bad-box'}'>"
            f"<b>Center-form equivalence {'holds' if ok else 'FAILS'}:</b> "
            f"<code>regime=auto, corr={CORR_EQUIV}</code> differs from forced "
            f"<code>regime=multiplicative</code> by {equiv}. "
            f"{'The corr_threshold gradient is therefore taken at a legitimately equivalent center.' if ok else 'The corr_threshold slope below is NOT comparable to the others.'}</p>")
    else:
        equiv_box = ("<p class='verdict bad-box'>Equivalence check missing "
                     "(center_auto_corr did not build).</p>")

    dec_drift = base["dec15"] - DEC_CANONICAL
    pairs = compensation_pairs(df)
    pairs.to_csv(HERE / "s01_gradient_pairs.csv", index=False)

    figs = [
        fig_tornado(df, "trough", "Sensitivity of the Aug-25 trough minimum", "DAU"),
        fig_tornado(df, "dec15", "Sensitivity of Dec-15 (the constrained quantity)", "DAU"),
        fig_tornado(df, "seam_kink", "Sensitivity of the seam handoff kink", "DAU/day"),
        fig_tradeoff(df),
        fig_pairs(pairs),
    ]
    chart_html = [pio.to_html(f, include_plotlyjs=(i == 0), full_html=False,
                              config={"displaylogo": False})
                  for i, f in enumerate(figs)]

    best = df.iloc[0]

    # Ratio spread is the real feasibility test: parallel knobs cannot be combined.
    finite = df[df["gain_per_dec"] != float("inf")]
    ratio_lo = finite["gain_per_dec"].min() if not finite.empty else float("nan")
    ratio_hi = finite["gain_per_dec"].max() if not finite.empty else float("nan")
    plausible = pairs[pairs["plausible"] & (pairs["net_trough_gain"] > 0)]
    if not plausible.empty:
        bp = plausible.iloc[0]
        pair_txt = (
            f"<b>Best plausible pair:</b> drive <code>{bp['driver']}</code> "
            f"(+1 step) and compensate with <code>{bp['compensator']}</code> at "
            f"{bp['steps_compensator']:+.2f} steps → predicted "
            f"<b>{bp['net_trough_gain']:+,.0f}</b> trough at zero net Dec-15 drift.")
        pair_cls = "good-box"
    else:
        pair_txt = ("<b>No plausible pair produces a positive trough gain at zero "
                    "net Dec-15 drift</b> within ±"
                    f"{MAX_PLAUSIBLE_STEPS:.0f} compensator steps.")
        pair_cls = "bad-box"

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>s01 sensitivity gradient — August desktop</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, sans-serif;
         margin: 0 auto; max-width: 1400px; padding: 28px 32px 80px; color: #1a1a1a;
         line-height: 1.5; }}
  h1 {{ font-size: 25px; margin-bottom: 4px; }}
  h2 {{ font-size: 19px; margin-top: 40px; border-bottom: 2px solid #e0e0e0;
        padding-bottom: 6px; }}
  .sub {{ color: #666; font-size: 14px; margin-top: 0; }}
  .verdict {{ padding: 13px 17px; border-radius: 6px; font-size: 14.5px; }}
  .good-box {{ background: #e8f5e9; border-left: 5px solid #2e7d32; }}
  .bad-box  {{ background: #ffebee; border-left: 5px solid #c62828; }}
  .note {{ background: #f5f5f5; border-left: 5px solid #9e9e9e; padding: 12px 16px;
           font-size: 14px; border-radius: 4px; }}
  table {{ border-collapse: collapse; font-size: 12.5px; width: 100%;
           font-variant-numeric: tabular-nums; }}
  th {{ background: #f4f4f4; text-align: right; padding: 7px 9px;
        border-bottom: 2px solid #ccc; }}
  td {{ text-align: right; padding: 6px 9px; border-bottom: 1px solid #eee; }}
  td.name {{ text-align: left; font-family: ui-monospace, Menlo, monospace;
             font-size: 11.5px; }}
  td.dim {{ color: #777; }}
  .good {{ color: #2e7d32; font-weight: 600; }}
  .bad {{ color: #c62828; }}
  code {{ background: #f4f4f4; padding: 1px 5px; border-radius: 3px; font-size: 12px; }}
</style></head><body>

<h1>Sensitivity gradient at July's s01 center — August desktop</h1>
<p class="sub">Central differences on all {len(df)} numeric parameters. This is a
<b>sensitivity map, not an optimisation</b>: the center is expected to miss the Dec-15
criterion, and does.</p>

<p class="note"><b>Center</b> (July's s01 rebuilt on August data):
<code>regime=multiplicative, cps=0.1849, cpr=0.734, recent_weeks=17,
n_changepoints=35, sps=0.00825</code>, holiday knobs at defaults.<br>
Trough minimum <b>{base['trough']:,.0f}</b> on {base['trough_date']} ·
Aug-15 {base['aug15']:,.0f} · Dec-15 <b>{base['dec15']:,.0f}</b>
({dec_drift:+,.0f} vs canonical, {'inside' if abs(dec_drift) <= DEC_TOL else 'OUTSIDE'}
the ±{DEC_TOL:,} band) · seam kink {base['seam_kink']:+,.0f}/day.</p>

{equiv_box}

<p class="verdict note"><b>What to look for — and what NOT to conclude.</b> A knob that
moves Dec-15 is <i>not</i> disqualified: a second knob can cancel that drift. So the test
is not whether any single ratio is favourable, it is whether the ratios have
<b>spread</b>. Two knobs can be combined to gain trough at zero net Dec-15 exactly when
their direction vectors are non-parallel, i.e. when their trough:Dec ratios differ — the
net gain is (dT<sub>A</sub>·dD<sub>B</sub> − dT<sub>B</sub>·dD<sub>A</sub>) / dD<sub>B</sub>.
Uniformly unfavourable ratios are survivable; uniformly <i>parallel</i> ones are not.
Observed ratio spread here: <b>{ratio_lo:.2f} … {ratio_hi:.2f}</b>. Largest single trough
slope: <code>{best['parameter']}</code> at {best['trough_step']:+,.0f} per
±{best['delta']} step.</p>

<h2>1. Trough sensitivity</h2>
{chart_html[0]}

<h2>2. Dec-15 sensitivity</h2>
{chart_html[1]}

<h2>3. Seam-kink sensitivity</h2>
{chart_html[2]}

<h2>4. Direction spread — what makes combination possible</h2>
{chart_html[3]}

<h2>5. Two-knob combinations at zero net Dec-15 drift</h2>
<p class="verdict {pair_cls}">{pair_txt}</p>
<p class="note"><b>These are first-order predictions, not results.</b> Superposition
assumes local linearity, and the grid already showed this space is not globally linear
(outcomes were bimodal, driven by an atomic tile flip). Every pair below is a hypothesis
that needs one confirming probe. Compensator step counts far from ±1 compound the risk,
because they walk that knob well outside the interval its slope was measured on — pairs
needing more than ±{MAX_PLAUSIBLE_STEPS:.0f} steps are excluded from the chart. Full
enumeration: <code>s01_gradient_pairs.csv</code></p>
{chart_html[4]}

<h2>6. Slope table</h2>
<p class="sub"><b>curvature</b> = f(x+δ) + f(x−δ) − 2f(x), the second difference. When it
is comparable to the slope itself the local-linear reading is unreliable and the response
is bending inside ±δ. Integer knobs bottom out at ±1 (so the holiday radii are ±20% and
±33% of center and remain secants, not derivatives). Raw values:
<code>s01_gradient_slopes.csv</code></p>
{build_table(df)}

</body></html>
"""
    OUT_HTML.write_text(html)
    print(f"\nWrote {OUT_HTML}\nWrote {OUT_CSV}")

    print(f"\ncenter: trough {base['trough']:,.0f} | Dec-15 {base['dec15']:,.0f} "
          f"({dec_drift:+,.0f})")
    print("\nslopes, ranked by trough effect per +10% of center "
          "(raw per-step columns flatter the coarsely-stepped integer knobs):")
    print(f"{'parameter':30s} {'d%':>5s} {'trough/step':>12s} {'Dec/step':>11s} "
          f"{'trough/+10%':>12s} {'Dec/+10%':>11s} {'ratio':>7s} {'same':>5s}")
    for _, r in df.iterrows():
        ratio = "inf" if r["gain_per_dec"] == float("inf") else f"{r['gain_per_dec']:.2f}"
        print(f"{r['parameter']:30s} {r['delta_pct_of_center']:>4.0f}% "
              f"{r['trough_step']:>+12,.0f} {r['dec15_step']:>+11,.0f} "
              f"{r['trough_per_10pct']:>+12,.0f} {r['dec15_per_10pct']:>+11,.0f} "
              f"{ratio:>7s} {'yes' if r['dec_moves_same_way'] else 'NO':>5s}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
