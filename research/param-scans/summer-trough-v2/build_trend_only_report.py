#!/usr/bin/env python3
"""Report for the trend-only grid (holiday parameters permanently excluded).

Scores every probe under `trend_only/` plus the s01 center and the canonical
reference, and answers one question: **what is the best Aug-25 trough achievable
with Dec-15 inside ±50,000, using only the trend/seasonality knobs?**

Charts: the feasibility frontier (Dec-15 vs trough with the band shaded), the
measured `changepoint_range` response (whose gradient was untrustworthy —
curvature 2.4× its own slope — so it had to be mapped rather than extrapolated),
and the cpr × ncp surface.

Usage
-----
    source .venv/bin/activate
    python research/param-scans/summer-trough-v2/build_trend_only_report.py
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

HERE = REPO_ROOT / "research/param-scans/summer-trough-v2"
GRID_DIR = HERE / "trend_only"
OUT_HTML = HERE / "trend_only_report.html"
OUT_CSV = HERE / "trend_only_scores.csv"
FORECAST_START = "2026-07-28"
PARQUET_NAME = f"mozaic_daily_forecast.{FORECAST_START}.ld-D.adj-lo.parquet"
HEADWIND = json.loads(
    (REPO_ROOT / "data-official/2026-08/adjustments/headwind.json").read_text())

DEC_CANONICAL = 48_672_970
DEC_TOL = 50_000
DEC_LO, DEC_HI = DEC_CANONICAL - DEC_TOL, DEC_CANONICAL + DEC_TOL
TROUGH_CANONICAL = 43_833_674
TARGET_BAND = (45_000_000, 46_000_000)
AUG15, AUG25 = "2026-08-15", "2026-08-25"

S01_SLUG = "cps0.1849_thresh032_recent17_cpr0.734_ncp35_clip0.6_sps0.00825_regimemultiplicative"
REFERENCES = {
    "canonical (auto)": REPO_ROOT / "data-official/2026-08/desktop_baseline_2026-07-28"
    / "cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825",
    "s01 center": HERE / "s01_gradient" / S01_SLUG,
}
TRACKED = ["prophet_changepoint_prior_scale", "prophet_changepoint_range",
           "prophet_recent_weeks", "prophet_n_changepoints",
           "prophet_seasonality_prior_scale", "seasonality_corr_threshold",
           "seasonality_regime"]
SHORT = {"prophet_changepoint_prior_scale": "cps", "prophet_changepoint_range": "cpr",
         "prophet_recent_weeks": "recent", "prophet_n_changepoints": "ncp",
         "prophet_seasonality_prior_scale": "sps",
         "seasonality_corr_threshold": "corr", "seasonality_regime": "regime"}
HOLIDAY_FIELDS = ["holiday_threshold", "holiday_max_radius",
                  "holiday_min_radius", "holiday_effect_floor"]
HOLIDAY_DEFAULTS = {"holiday_threshold": -0.032, "holiday_max_radius": 5,
                    "holiday_min_radius": 3, "holiday_effect_floor": -0.6}


def score_build(build_dir: Path, label: str | None = None) -> dict | None:
    parquet, params = build_dir / PARQUET_NAME, build_dir / "parameters.json"
    if not (parquet.exists() and params.exists()):
        return None
    df, _ = load_forecast(str(parquet))
    at25 = score_dataframe(df, target_date=AUG25, headwind_spec=HEADWIND)
    at15 = score_dataframe(df, target_date=AUG15, headwind_spec=HEADWIND)
    cfg = json.loads(params.read_text())["config"]

    row = {"label": label or build_dir.name, "slug": build_dir.name,
           "aug15": at15["global_target_post"], "aug25": at25["global_target_post"],
           "trough": at25["trough_min_post"], "trough_date": at25["trough_min_date"],
           "dec15": at25["global_dec15_post"],
           "seam_kink": at25["seam_slope_kink_model"]}
    row.update({k: cfg.get(k) for k in TRACKED})
    row["dec_drift"] = row["dec15"] - DEC_CANONICAL
    row["trough_gain"] = row["trough"] - TROUGH_CANONICAL
    row["in_dec_band"] = DEC_LO <= row["dec15"] <= DEC_HI
    row["in_target_band"] = TARGET_BAND[0] <= row["trough"] <= TARGET_BAND[1]
    # Integrity guard: this search must not have touched holiday knobs.
    row["holiday_clean"] = all(cfg.get(f) == HOLIDAY_DEFAULTS[f] for f in HOLIDAY_FIELDS)
    return row


def collect() -> pd.DataFrame:
    rows = []
    for label, path in REFERENCES.items():
        row = score_build(path, label=label)
        if row:
            row["kind"] = "reference"
            rows.append(row)
        else:
            print(f"  WARNING: reference missing: {path}")
    if GRID_DIR.exists():
        for d in sorted(GRID_DIR.iterdir()):
            if not d.is_dir() or d.name == "logs":
                continue
            row = score_build(d)
            if row:
                row["kind"] = "probe"
                rows.append(row)
            else:
                print(f"  WARNING: incomplete probe: {d.name}")
    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("nothing scored -- has the grid run?")
    return df.sort_values("trough", ascending=False).reset_index(drop=True)


def delta_str(row: pd.Series, base: dict) -> str:
    parts = [f"{SHORT[k]}={row[k]}" for k in TRACKED if row.get(k) != base.get(k)]
    return ", ".join(parts) if parts else "center"


def hover(row: pd.Series, base: dict) -> str:
    return (f"<b>{delta_str(row, base)}</b><br>"
            f"trough {row['trough']:,.0f} ({row['trough_gain']:+,.0f})<br>"
            f"Aug-15 {row['aug15']:,.0f}<br>"
            f"Dec-15 {row['dec15']:,.0f} ({row['dec_drift']:+,.0f})<br>"
            f"seam kink {row['seam_kink']:+,.0f}/day<br>"
            f"Dec band: {'INSIDE' if row['in_dec_band'] else 'outside'}")


def fig_frontier(df: pd.DataFrame, base: dict) -> go.Figure:
    fig = go.Figure()
    fig.add_vrect(x0=DEC_LO, x1=DEC_HI, fillcolor="#2e7d32", opacity=0.13,
                  line_width=0, layer="below",
                  annotation_text="Dec-15 band (±50k)", annotation_position="top left")
    fig.add_hrect(y0=TARGET_BAND[0], y1=TARGET_BAND[1], fillcolor="#1565c0",
                  opacity=0.07, line_width=0, layer="below",
                  annotation_text="trough target 45–46M", annotation_position="top right")
    probes = df[df["kind"] == "probe"]
    fig.add_trace(go.Scatter(
        x=probes["dec15"], y=probes["trough"], mode="markers", name="probes",
        marker=dict(size=11, color=probes["prophet_changepoint_range"],
                    colorscale="Plasma", showscale=True, line=dict(width=1, color="#333"),
                    colorbar=dict(title="cpr", x=1.02, len=0.8)),
        text=[hover(r, base) for _, r in probes.iterrows()], hoverinfo="text"))
    for _, r in df[df["kind"] == "reference"].iterrows():
        fig.add_trace(go.Scatter(
            x=[r["dec15"]], y=[r["trough"]], mode="markers+text", name=r["label"],
            text=[r["label"]], textposition="bottom center",
            marker=dict(size=18, symbol="star",
                        color="#c62828" if "s01" in r["label"] else "#555",
                        line=dict(width=1.5, color="#fff")),
            hovertext=[hover(r, base)], hoverinfo="text"))
    fig.update_layout(
        title="Feasibility frontier — best result is the highest point inside the green band",
        xaxis_title="Dec-15 28d-MA, post-headwind (DAU)",
        yaxis_title="Aug trough minimum, post-headwind (DAU)",
        height=620, template="plotly_white", legend=dict(orientation="h", y=-0.16))
    fig.update_xaxes(tickformat=",.4s")
    fig.update_yaxes(tickformat=",.4s")
    return fig


def fig_cpr_response(df: pd.DataFrame) -> go.Figure:
    """Measured cpr response. Its gradient was unusable (curvature 2.4x slope)."""
    d = df[(df["kind"] != "reference")
           | (df["label"] == "s01 center")].copy()
    base_ncp = 35
    d = d[d["prophet_n_changepoints"] == base_ncp].sort_values("prophet_changepoint_range")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=d["prophet_changepoint_range"], y=d["trough"],
                            mode="lines+markers", name="trough",
                            line=dict(color="#1565c0", width=2.5), marker=dict(size=9)))
    fig.add_trace(go.Scatter(x=d["prophet_changepoint_range"], y=d["dec15"],
                            mode="lines+markers", name="Dec-15", yaxis="y2",
                            line=dict(color="#c62828", width=2.5, dash="dash"),
                            marker=dict(size=9)))
    fig.add_shape(type="rect", xref="paper", yref="y2", x0=0, x1=1,
                  y0=DEC_LO, y1=DEC_HI, fillcolor="#2e7d32", opacity=0.12,
                  line_width=0, layer="below")
    fig.update_layout(
        title=f"Measured changepoint_range response (ncp={base_ncp}) — mapped, not "
              f"extrapolated, because its curvature was 2.4× its own slope",
        xaxis_title="changepoint_range",
        yaxis=dict(title=dict(text="trough (DAU)", font=dict(color="#1565c0")),
                   tickfont=dict(color="#1565c0"), tickformat=",.4s"),
        yaxis2=dict(title=dict(text="Dec-15 (DAU)", font=dict(color="#c62828")),
                    tickfont=dict(color="#c62828"), overlaying="y", side="right",
                    tickformat=",.4s"),
        height=540, template="plotly_white", hovermode="x unified",
        legend=dict(orientation="h", y=-0.18))
    return fig


def fig_surface(df: pd.DataFrame, base: dict) -> go.Figure:
    """cpr x ncp: the compensating pair. Marker ring = inside the Dec-15 band."""
    d = df[df["kind"] == "probe"]
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=d["prophet_changepoint_range"], y=d["prophet_n_changepoints"],
        mode="markers", marker=dict(
            size=[20 if v else 12 for v in d["in_dec_band"]],
            color=d["trough"], colorscale="Viridis", showscale=True,
            colorbar=dict(title="trough"),
            line=dict(width=[3 if v else 1 for v in d["in_dec_band"]],
                      color=["#2e7d32" if v else "#999" for v in d["in_dec_band"]])),
        text=[hover(r, base) for _, r in d.iterrows()], hoverinfo="text"))
    fig.update_layout(
        title="cpr × ncp — the compensating pair (cpr lowers Dec-15, ncp raises it). "
              "Green ring = Dec-15 inside the band.",
        xaxis_title="changepoint_range", yaxis_title="n_changepoints",
        height=540, template="plotly_white")
    return fig


def build_table(df: pd.DataFrame, base: dict) -> str:
    head = ["config", "trough", "gain", "Aug-15", "Dec-15", "Dec drift",
            "seam kink", "Dec band", "45–46M"]
    rows = []
    for _, r in df.iterrows():
        cls = "inband" if r["in_dec_band"] else "outband"
        if r["kind"] == "reference":
            cls += " ref"
        rows.append(
            f"<tr class='{cls}'>"
            f"<td class='name'>{r['label'] if r['kind']=='reference' else delta_str(r, base)}</td>"
            f"<td><b>{r['trough']:,.0f}</b></td>"
            f"<td>{r['trough_gain']:+,.0f}</td>"
            f"<td>{r['aug15']:,.0f}</td>"
            f"<td>{r['dec15']:,.0f}</td>"
            f"<td class='{'good' if r['in_dec_band'] else 'bad'}'>{r['dec_drift']:+,.0f}</td>"
            f"<td>{r['seam_kink']:+,.0f}</td>"
            f"<td>{'✓' if r['in_dec_band'] else '✗'}</td>"
            f"<td>{'✓' if r['in_target_band'] else '✗'}</td>"
            "</tr>")
    return ("<table><thead><tr>" + "".join(f"<th>{h}</th>" for h in head)
            + "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>")


def main() -> int:
    print("Scoring trend-only grid...")
    df = collect()
    df.to_csv(OUT_CSV, index=False)
    print(f"  scored {len(df)} builds")

    dirty = df[~df["holiday_clean"]]
    if not dirty.empty:
        raise SystemExit(
            "INTEGRITY FAILURE: holiday knobs are off-default in "
            f"{len(dirty)} build(s): {list(dirty['label'])}. This search excludes them.")

    s01 = df[df["label"] == "s01 center"]
    base = s01.iloc[0].to_dict() if not s01.empty else {}

    feasible = df[df["in_dec_band"]]
    best = feasible.nlargest(1, "trough").iloc[0]
    improves = best["trough"] > base.get("trough", 0) + 1_000

    figs = [fig_frontier(df, base), fig_cpr_response(df), fig_surface(df, base)]
    charts = [pio.to_html(f, include_plotlyjs=(i == 0), full_html=False,
                          config={"displaylogo": False}) for i, f in enumerate(figs)]

    verdict = (
        f"<p class='verdict {'good-box' if improves else 'warn-box'}'>"
        f"<b>Best trend-only config holding Dec-15:</b> "
        f"<code>{delta_str(best, base) if best['kind']!='reference' else best['label']}</code><br>"
        f"trough <b>{best['trough']:,.0f}</b> ({best['trough_gain']:+,.0f} vs canonical) · "
        f"Dec-15 {best['dec15']:,.0f} ({best['dec_drift']:+,.0f}) · "
        f"seam kink {best['seam_kink']:+,.0f}/day<br>"
        + (f"Improves on the s01 center by <b>{best['trough'] - base['trough']:+,.0f}</b>."
           if improves else
           "<b>No probe beat the s01 center</b> — it remains the best trend-only result.")
        + "</p>")

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>Trend-only grid — August desktop summer trough</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, sans-serif;
         margin: 0 auto; max-width: 1400px; padding: 28px 32px 80px; color: #1a1a1a;
         line-height: 1.5; }}
  h1 {{ font-size: 25px; margin-bottom: 4px; }}
  h2 {{ font-size: 19px; margin-top: 40px; border-bottom: 2px solid #e0e0e0;
        padding-bottom: 6px; }}
  .sub {{ color: #666; font-size: 14px; margin-top: 0; }}
  .verdict {{ padding: 14px 18px; border-radius: 6px; font-size: 15px; }}
  .good-box {{ background: #e8f5e9; border-left: 5px solid #2e7d32; }}
  .warn-box {{ background: #fff8e1; border-left: 5px solid #f9a825; }}
  .note {{ background: #f5f5f5; border-left: 5px solid #9e9e9e; padding: 12px 16px;
           font-size: 14px; border-radius: 4px; }}
  table {{ border-collapse: collapse; font-size: 12.5px; width: 100%;
           font-variant-numeric: tabular-nums; }}
  th {{ background: #f4f4f4; text-align: right; padding: 7px 9px;
        border-bottom: 2px solid #ccc; }}
  td {{ text-align: right; padding: 6px 9px; border-bottom: 1px solid #eee; }}
  td.name {{ text-align: left; font-family: ui-monospace, Menlo, monospace;
             font-size: 11.5px; }}
  tr.inband {{ background: #f1f8f2; }}
  tr.ref {{ font-weight: 600; background: #eef4fb; }}
  .good {{ color: #2e7d32; font-weight: 600; }}
  .bad {{ color: #c62828; }}
  code {{ background: #f4f4f4; padding: 1px 5px; border-radius: 3px; font-size: 12px; }}
</style></head><body>

<h1>Trend-only grid — August desktop summer trough</h1>
<p class="sub">Best Aug-25 trough achievable with Dec-15 inside
[{DEC_LO:,}, {DEC_HI:,}], using <b>only</b> trend/seasonality knobs.
{len(df)} builds scored.</p>

<p class="note"><b>Holiday parameters are permanently excluded.</b> They produce
strictly local effects (a few days around detected holidays), so using them to move a
whole-season quantity is compensating for an overall trend with a small regional fix —
overfitting. All four are pinned to package defaults, verified per build before this
report renders. For the record, the s01 gradient measured
<code>holiday_max_radius</code> as the second-largest trough lever and
<code>holiday_threshold</code> as the best trough:Dec ratio; both were declined on
principle, not on evidence.</p>

{verdict}

<h2>1. Feasibility frontier</h2>
{charts[0]}

<h2>2. The dominant lever, measured</h2>
{charts[1]}

<h2>3. cpr × ncp</h2>
{charts[2]}

<h2>4. Every build</h2>
<p class="sub">Sorted by trough. Green rows hold Dec-15; bold rows are references.
Raw scores: <code>trend_only_scores.csv</code></p>
{build_table(df, base)}

</body></html>
"""
    OUT_HTML.write_text(html)
    print(f"\nWrote {OUT_HTML}\nWrote {OUT_CSV}")
    if base:
        print(f"s01 center : trough {base['trough']:,.0f} | "
              f"Dec-15 {base['dec15']:,.0f} ({base['dec_drift']:+,.0f})")
    print(f"BEST in-band: trough {best['trough']:,.0f} "
          f"({best['trough_gain']:+,.0f}) | Dec-15 {best['dec15']:,.0f} "
          f"({best['dec_drift']:+,.0f}) | {best['slug']}")
    print(f"\nin Dec band: {int(df['in_dec_band'].sum())}/{len(df)} | "
          f"in 45-46M trough band: {int(df['in_target_band'].sum())}/{len(df)} | "
          f"BOTH: {int((df['in_dec_band'] & df['in_target_band']).sum())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
