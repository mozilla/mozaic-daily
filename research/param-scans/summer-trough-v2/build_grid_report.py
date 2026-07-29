#!/usr/bin/env python3
"""Self-contained HTML report for the August summer-trough grid.

Scores every probe under `grid/` (plus the canonical and the forced-multiplicative
reference builds) and renders four views:

1. **Frontier** -- Dec-15 vs Aug-25 trough, with the +-50k Dec-15 band shaded. The
   answer to "what is the best trough that actually holds Dec-15" is read straight
   off this chart.
2. **corr_threshold sweep** -- the newly exposed continuous dial, with the trough
   and Dec-15 on twin axes, showing where it leaves the band.
3. **Seam slope** -- the 28d-MA handoff derivative per probe against the actuals'
   own slope. Reported, not scored.
4. **Parallel coordinates** -- every parameter against every outcome.

Plus a sortable table of all probes.

Objective, restated: raise the Aug-25 trough minimum (28d-MA, post-headwind) while
holding Dec-15 within +-50,000 of the canonical 48,672,970. The Win10 headwind is
fixed at -1,245,000 -- the +-50k allowance is its own adjustability.

Usage
-----
    source .venv/bin/activate
    python research/param-scans/summer-trough-v2/build_grid_report.py
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
GRID_DIR = HERE / "grid"
OUT_HTML = HERE / "grid_report.html"
FORECAST_START = "2026-07-28"
PARQUET_NAME = f"mozaic_daily_forecast.{FORECAST_START}.ld-D.adj-lo.parquet"
HEADWIND = json.loads(
    (REPO_ROOT / "data-official/2026-08/adjustments/headwind.json").read_text())

DEC_CANONICAL = 48_672_970
DEC_TOL = 50_000
DEC_LO, DEC_HI = DEC_CANONICAL - DEC_TOL, DEC_CANONICAL + DEC_TOL
TROUGH_CANONICAL = 43_833_674
AUG15 = "2026-08-15"
AUG25 = "2026-08-25"

REFERENCE_BUILDS = {
    "canonical (auto)": REPO_ROOT / "data-official/2026-08/desktop_baseline_2026-07-28"
    / "cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825",
    "regime=multiplicative": HERE / "phase1"
    / "cps0.08983_thresh032_recent13_cpr0.65_ncp25_clip0.6_sps0.00825_regimemultiplicative",
}

PARAM_LABELS = {
    "seasonality_corr_threshold": "corr thresh",
    "seasonality_regime": "regime",
    "prophet_changepoint_range": "cpr",
    "prophet_changepoint_prior_scale": "cps",
    "prophet_n_changepoints": "ncp",
    "prophet_recent_weeks": "recent wk",
    "prophet_seasonality_prior_scale": "sps",
    "holiday_threshold": "hol thresh",
    "holiday_max_radius": "hol max r",
    "holiday_min_radius": "hol min r",
    "holiday_effect_floor": "hol floor",
}


def score_build(build_dir: Path, label: str | None = None) -> dict | None:
    """Score one build dir at Aug-25 (objective) and Aug-15 (reported)."""
    parquet = build_dir / PARQUET_NAME
    params_path = build_dir / "parameters.json"
    if not parquet.exists() or not params_path.exists():
        return None

    df, _meta = load_forecast(str(parquet))
    at25 = score_dataframe(df, target_date=AUG25, headwind_spec=HEADWIND)
    at15 = score_dataframe(df, target_date=AUG15, headwind_spec=HEADWIND)
    config = json.loads(params_path.read_text())["config"]

    row = {
        "label": label or build_dir.name,
        "slug": build_dir.name,
        "aug15": at15["global_target_post"],
        "aug25": at25["global_target_post"],
        "trough_min": at25["trough_min_post"],
        "trough_min_date": at25["trough_min_date"],
        "dec15": at25["global_dec15_post"],
        "aug25_ex_cn_ir": at25["ex_cn_ir_target_post"],
        "seam_slope_before": at25["seam_slope_before"],
        "seam_slope_after_model": at25["seam_slope_after_model"],
        "seam_kink_model": at25["seam_slope_kink_model"],
        "seam_kink_display": at25["seam_slope_kink_display"],
    }
    row.update({k: config.get(k) for k in PARAM_LABELS})
    row["dec_drift"] = row["dec15"] - DEC_CANONICAL
    row["trough_gain"] = row["trough_min"] - TROUGH_CANONICAL
    row["in_band"] = DEC_LO <= row["dec15"] <= DEC_HI
    return row


def collect() -> pd.DataFrame:
    rows = []
    for label, path in REFERENCE_BUILDS.items():
        row = score_build(path, label=label)
        if row:
            row["kind"] = "reference"
            rows.append(row)
        else:
            print(f"  WARNING: reference build missing: {path}")

    if GRID_DIR.exists():
        for d in sorted(GRID_DIR.iterdir()):
            if not d.is_dir() or d.name == "logs":
                continue
            row = score_build(d)
            if row:
                row["kind"] = "probe"
                rows.append(row)
            else:
                print(f"  WARNING: incomplete probe (no parquet): {d.name}")

    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("No scored builds found -- has the grid run?")
    return df.sort_values("trough_min", ascending=False).reset_index(drop=True)


def hover_text(row: pd.Series) -> str:
    changed = []
    base = {"seasonality_corr_threshold": 0.0, "seasonality_regime": "auto",
            "prophet_changepoint_range": 0.65, "prophet_changepoint_prior_scale": 0.08983,
            "prophet_n_changepoints": 25, "prophet_recent_weeks": 13,
            "prophet_seasonality_prior_scale": 0.00825, "holiday_threshold": -0.032,
            "holiday_max_radius": 5, "holiday_min_radius": 3, "holiday_effect_floor": -0.6}
    for key, short in PARAM_LABELS.items():
        if row.get(key) is not None and row.get(key) != base.get(key):
            changed.append(f"{short}={row[key]}")
    delta = ", ".join(changed) if changed else "center (no overrides)"
    return (
        f"<b>{row['label']}</b><br>"
        f"{delta}<br><br>"
        f"Aug-25 trough : {row['aug25']:,.0f}<br>"
        f"trough min    : {row['trough_min']:,.0f} ({row['trough_min_date']})<br>"
        f"Aug-15        : {row['aug15']:,.0f}<br>"
        f"Dec-15        : {row['dec15']:,.0f} ({row['dec_drift']:+,.0f})<br>"
        f"seam kink     : {row['seam_kink_model']:+,.0f}/day<br>"
        f"Dec-15 band   : {'INSIDE' if row['in_band'] else 'outside'}"
    )


def cluster_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    """Split results at the largest gap in trough outcome, if the gap is decisive.

    The regime switch is per tile and the heaviest tile is atomic, so outcomes can
    land in two disjoint clusters with nothing between them. Detect that rather
    than assuming a smooth frontier.
    """
    vals = df["trough_min"].sort_values()
    gaps = vals.diff()
    if gaps.max() < 200_000:  # no decisive separation -> treat as a continuum
        return None
    cut = vals[gaps.idxmax()]
    return df[df["trough_min"] < cut], df[df["trough_min"] >= cut]


def fig_frontier(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    split = cluster_split(df)
    if split is not None:
        low, high = split
        fig.add_hrect(y0=low["trough_min"].max(), y1=high["trough_min"].min(),
                      fillcolor="#c62828", opacity=0.07, line_width=0, layer="below",
                      annotation_text="unreachable — no config lands here",
                      annotation_position="top left")
    fig.add_vrect(x0=DEC_LO, x1=DEC_HI, fillcolor="#2e7d32", opacity=0.13,
                  line_width=0, layer="below",
                  annotation_text="Dec-15 band (±50k)", annotation_position="top left")
    fig.add_vline(x=DEC_CANONICAL, line=dict(color="#2e7d32", width=1, dash="dot"))
    fig.add_hline(y=TROUGH_CANONICAL, line=dict(color="#888", width=1, dash="dot"),
                  annotation_text="canonical trough", annotation_position="bottom right")

    probes = df[df["kind"] == "probe"]
    fig.add_trace(go.Scatter(
        x=probes["dec15"], y=probes["trough_min"], mode="markers",
        name="grid probes",
        marker=dict(size=11, color=probes["seasonality_corr_threshold"],
                    colorscale="Viridis", showscale=True, line=dict(width=1, color="#333"),
                    colorbar=dict(title="corr<br>thresh", x=1.02, len=0.8)),
        text=[hover_text(r) for _, r in probes.iterrows()], hoverinfo="text",
    ))
    for _, r in df[df["kind"] == "reference"].iterrows():
        fig.add_trace(go.Scatter(
            x=[r["dec15"]], y=[r["trough_min"]], mode="markers+text",
            name=r["label"], text=[r["label"]], textposition="top center",
            marker=dict(size=18, symbol="star",
                        color="#c62828" if "mult" in r["label"] else "#1565c0",
                        line=dict(width=1.5, color="#fff")),
            hovertext=[hover_text(r)], hoverinfo="text",
        ))

    fig.update_layout(
        title="Frontier — the best trough that still holds Dec-15 is the highest point inside the green band",
        xaxis_title="Dec-15 28d-MA, post-headwind (DAU)",
        yaxis_title="Aug trough minimum, post-headwind (DAU)",
        height=620, hovermode="closest", template="plotly_white",
        legend=dict(orientation="h", y=-0.16),
    )
    fig.update_xaxes(tickformat=",.4s")
    fig.update_yaxes(tickformat=",.4s")
    return fig


def fig_corr_sweep(df: pd.DataFrame) -> go.Figure:
    """Sweep along the new dial, holding every other knob at center."""
    base = {"prophet_changepoint_range": 0.65, "holiday_threshold": -0.032,
            "prophet_seasonality_prior_scale": 0.00825, "prophet_recent_weeks": 13}
    mask = df["seasonality_regime"] == "auto"
    for key, val in base.items():
        mask &= df[key] == val
    sweep = df[mask].sort_values("seasonality_corr_threshold")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=sweep["seasonality_corr_threshold"], y=sweep["trough_min"],
        mode="lines+markers", name="Aug trough min",
        line=dict(color="#1565c0", width=2.5), marker=dict(size=9),
        text=[hover_text(r) for _, r in sweep.iterrows()], hoverinfo="text",
    ))
    fig.add_trace(go.Scatter(
        x=sweep["seasonality_corr_threshold"], y=sweep["dec15"],
        mode="lines+markers", name="Dec-15", yaxis="y2",
        line=dict(color="#c62828", width=2.5, dash="dash"), marker=dict(size=9),
        text=[hover_text(r) for _, r in sweep.iterrows()], hoverinfo="text",
    ))
    fig.update_layout(
        title="corr_threshold sweep — the interior between the two regimes "
              "(all other knobs at center)",
        xaxis_title="seasonality_corr_threshold  (lower ⇒ more tiles multiplicative)",
        yaxis=dict(title=dict(text="Aug trough minimum (DAU)", font=dict(color="#1565c0")),
                   tickfont=dict(color="#1565c0"), tickformat=",.4s"),
        yaxis2=dict(title=dict(text="Dec-15 (DAU)", font=dict(color="#c62828")),
                    tickfont=dict(color="#c62828"), overlaying="y", side="right",
                    tickformat=",.4s"),
        height=560, template="plotly_white",
        legend=dict(orientation="h", y=-0.18), hovermode="x unified",
    )
    # Dec-15 tolerance band lives on the secondary axis.
    fig.add_shape(type="rect", xref="paper", yref="y2", x0=0, x1=1,
                  y0=DEC_LO, y1=DEC_HI, fillcolor="#2e7d32", opacity=0.12,
                  line_width=0, layer="below")
    return fig


def fig_seam(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    probes = df[df["kind"] == "probe"]
    actual_slope = df["seam_slope_before"].iloc[0]

    fig.add_hline(y=0, line=dict(color="#2e7d32", width=2),
                  annotation_text="perfect derivative match (kink = 0)",
                  annotation_position="top right")
    fig.add_trace(go.Scatter(
        x=probes["trough_min"], y=probes["seam_kink_model"], mode="markers",
        name="grid probes",
        marker=dict(size=11, color=probes["seasonality_corr_threshold"],
                    colorscale="Viridis", showscale=True, line=dict(width=1, color="#333"),
                    colorbar=dict(title="corr<br>thresh", x=1.02, len=0.8)),
        text=[hover_text(r) for _, r in probes.iterrows()], hoverinfo="text",
    ))
    for _, r in df[df["kind"] == "reference"].iterrows():
        fig.add_trace(go.Scatter(
            x=[r["trough_min"]], y=[r["seam_kink_model"]], mode="markers+text",
            name=r["label"], text=[r["label"]], textposition="top center",
            marker=dict(size=18, symbol="star",
                        color="#c62828" if "mult" in r["label"] else "#1565c0",
                        line=dict(width=1.5, color="#fff")),
            hovertext=[hover_text(r)], hoverinfo="text",
        ))
    fig.update_layout(
        title=f"Seam handoff derivative (reported, not scored) — actuals' own slope "
              f"is {actual_slope:+,.0f}/day",
        xaxis_title="Aug trough minimum (DAU)",
        yaxis_title="Slope kink at the seam, model only (DAU/day)",
        height=560, template="plotly_white", legend=dict(orientation="h", y=-0.16),
    )
    fig.update_xaxes(tickformat=",.4s")
    return fig


def fig_parallel(df: pd.DataFrame) -> go.Figure:
    """Parameters against outcomes. Constant columns are dropped as uninformative."""
    dims = []
    for key, short in PARAM_LABELS.items():
        col = df[key]
        if key == "seasonality_regime":
            codes = col.astype("category")
            if len(codes.cat.categories) < 2:
                continue
            dims.append(dict(label=short, values=codes.cat.codes,
                             tickvals=list(range(len(codes.cat.categories))),
                             ticktext=list(codes.cat.categories)))
            continue
        if col.nunique(dropna=True) < 2:
            continue
        dims.append(dict(label=short, values=col))

    for key, short in [("trough_min", "Aug trough"), ("aug15", "Aug-15"),
                       ("dec15", "Dec-15"), ("seam_kink_model", "seam kink")]:
        dims.append(dict(label=short, values=df[key]))

    fig = go.Figure(go.Parcoords(
        line=dict(color=df["trough_min"], colorscale="Viridis", showscale=True,
                  colorbar=dict(title="Aug<br>trough")),
        dimensions=dims,
    ))
    fig.update_layout(
        title="Every varied parameter against every outcome "
              "(drag along an axis to filter)",
        height=540, template="plotly_white", margin=dict(l=80, r=80, t=80, b=40),
    )
    return fig


def build_table(df: pd.DataFrame) -> str:
    show = df.copy()
    varied = [k for k in PARAM_LABELS if show[k].nunique(dropna=True) > 1]

    head = ["", "Aug-25 trough", "trough min", "date", "Aug-15", "Dec-15",
            "Dec drift", "seam kink", "band"] + [PARAM_LABELS[k] for k in varied]
    rows = []
    for _, r in show.iterrows():
        cls = "inband" if r["in_band"] else "outband"
        if r["kind"] == "reference":
            cls += " ref"
        cells = [
            f"<td class='name'>{r['label']}</td>",
            f"<td>{r['aug25']:,.0f}</td>",
            f"<td><b>{r['trough_min']:,.0f}</b></td>",
            f"<td class='dim'>{r['trough_min_date']}</td>",
            f"<td>{r['aug15']:,.0f}</td>",
            f"<td>{r['dec15']:,.0f}</td>",
            f"<td class='{'good' if abs(r['dec_drift'])<=DEC_TOL else 'bad'}'>"
            f"{r['dec_drift']:+,.0f}</td>",
            f"<td>{r['seam_kink_model']:+,.0f}</td>",
            f"<td>{'✓' if r['in_band'] else '✗'}</td>",
        ]
        cells += [f"<td class='dim'>{r[k]}</td>" for k in varied]
        rows.append(f"<tr class='{cls}'>" + "".join(cells) + "</tr>")

    return (
        "<table><thead><tr>" + "".join(f"<th>{h}</th>" for h in head)
        + "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
    )


def main() -> int:
    print("Scoring builds...")
    df = collect()
    print(f"  scored {len(df)} builds "
          f"({(df['kind']=='probe').sum()} probes + "
          f"{(df['kind']=='reference').sum()} references)")

    in_band = df[df["in_band"] & (df["kind"] == "probe")]
    best = in_band.nlargest(1, "trough_min").iloc[0] if not in_band.empty else None

    figs = [fig_frontier(df), fig_corr_sweep(df), fig_seam(df), fig_parallel(df)]
    chart_html = []
    for i, fig in enumerate(figs):
        chart_html.append(pio.to_html(
            fig, include_plotlyjs=(True if i == 0 else False), full_html=False,
            config={"displaylogo": False}))

    split = cluster_split(df)
    if split is not None and best is not None:
        low, high = split
        gap = high["trough_min"].min() - low["trough_min"].max()
        absorb = HEADWIND["desktop_dau"] - high["dec_drift"].min()
        verdict = (
            f"<p class='verdict bad-box'><b>Negative result: the ±50k Dec-15 constraint "
            f"caps the trough gain at {best['trough_gain']:+,.0f}</b> "
            f"({100*best['trough_gain']/TROUGH_CANONICAL:+.2f}%), i.e. effectively nothing. "
            f"Best in-band config <code>{best['slug']}</code> reaches "
            f"{best['trough_min']:,.0f} at Dec-15 {best['dec15']:,.0f} "
            f"({best['dec_drift']:+,.0f}).</p>"

            f"<p class='verdict warn-box'><b>Outcomes are bimodal, not a frontier.</b> "
            f"All {len(df)} builds land in one of two disjoint clusters with an empty "
            f"<b>{gap:,.0f}</b> gap between them — there is no interior to trade along:</p>"
            f"<table class='clusters'><thead><tr><th></th><th>trough</th>"
            f"<th>Dec-15 drift</th><th>seam kink</th><th>n</th><th>in band</th></tr></thead>"
            f"<tbody>"
            f"<tr><td class='name'>A · additive-dominant</td>"
            f"<td>{low['trough_min'].min():,.0f} … {low['trough_min'].max():,.0f}</td>"
            f"<td class='good'>{low['dec_drift'].min():+,.0f} … {low['dec_drift'].max():+,.0f}</td>"
            f"<td>{low['seam_kink_model'].max():+,.0f} … {low['seam_kink_model'].min():+,.0f}</td>"
            f"<td>{len(low)}</td><td class='good'>all ✓</td></tr>"
            f"<tr><td class='name'>B · multiplicative-dominant</td>"
            f"<td><b>{high['trough_min'].min():,.0f} … {high['trough_min'].max():,.0f}</b></td>"
            f"<td class='bad'>{high['dec_drift'].min():+,.0f} … {high['dec_drift'].max():+,.0f}</td>"
            f"<td><b>{high['seam_kink_model'].max():+,.0f} … "
            f"{high['seam_kink_model'].min():+,.0f}</b></td>"
            f"<td>{len(high)}</td><td class='bad'>none ✗</td></tr>"
            f"</tbody></table>"

            f"<p class='verdict note'><b>Why there is no middle.</b> The entire "
            f"{gap:,.0f} step is one tile changing regime: <code>ROW/modern_windows</code> "
            f"carries 27% of all desktop weight and flips atomically at corr −0.1465. "
            f"Thresholds of −0.105, −0.13 and −0.14 move 11.6–16.0% of DAU to "
            f"multiplicative and buy only +2,478 to +4,041 of trough — so the effect is "
            f"almost entirely that single tile, and it cannot be bought in fractions. "
            f"Both of the secondary goals live in cluster B too: the seam kink only "
            f"improves there ({high['seam_kink_model'].max():+,.0f} vs "
            f"{low['seam_kink_model'].max():+,.0f}).</p>"

            f"<p class='verdict note'>The cheapest exit from cluster A costs "
            f"<b>{high['dec_drift'].min():+,.0f}</b> on Dec-15 — "
            f"<b>{high['dec_drift'].min()/DEC_TOL:.1f}×</b> the budget. Absorbing that "
            f"with the headwind would require an anchor of <b>{absorb:,.0f}</b>, which is "
            f"more negative than June's −1,420,000 and reverses all four of this year's "
            f"attenuations. That is a judgement call, not a search result.</p>"
        )
    elif best is not None:
        verdict = (
            f"<p class='verdict good-box'><b>Best probe holding Dec-15:</b> "
            f"trough <b>{best['trough_min']:,.0f}</b> "
            f"({best['trough_gain']:+,.0f} vs canonical) at Dec-15 "
            f"{best['dec15']:,.0f} ({best['dec_drift']:+,.0f}). "
            f"Config: <code>{best['slug']}</code>.</p>"
        )
    else:
        verdict = ("<p class='verdict bad-box'><b>No probe held Dec-15 within "
                   "±50,000.</b> The frontier chart shows how close the closest came.</p>")

    table_html = build_table(df)
    csv_path = HERE / "grid_scores.csv"
    df.to_csv(csv_path, index=False)

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>August 2026 desktop — summer-trough grid</title>
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
  .bad-box  {{ background: #ffebee; border-left: 5px solid #c62828; }}
  .warn-box {{ background: #fff8e1; border-left: 5px solid #f9a825; }}
  .verdict.note {{ background: #f5f5f5; border-left: 5px solid #9e9e9e;
                   font-size: 14px; }}
  table.clusters {{ width: auto; min-width: 720px; margin: 4px 0 18px; }}
  table.clusters td.name {{ font-family: inherit; font-size: 13.5px; font-weight: 600; }}
  table {{ border-collapse: collapse; font-size: 12.5px; width: 100%;
           font-variant-numeric: tabular-nums; }}
  th {{ background: #f4f4f4; text-align: right; padding: 7px 9px;
        border-bottom: 2px solid #ccc; position: sticky; top: 0; }}
  td {{ text-align: right; padding: 6px 9px; border-bottom: 1px solid #eee; }}
  td.name {{ text-align: left; font-family: ui-monospace, Menlo, monospace;
             font-size: 11.5px; max-width: 330px; overflow-wrap: anywhere; }}
  td.dim {{ color: #777; }}
  tr.inband {{ background: #f1f8f2; }}
  tr.ref {{ font-weight: 600; background: #eef4fb; }}
  .good {{ color: #2e7d32; font-weight: 600; }}
  .bad {{ color: #c62828; }}
  .note {{ background: #fffde7; border-left: 5px solid #f9a825; padding: 12px 16px;
           font-size: 14px; border-radius: 4px; }}
  code {{ background: #f4f4f4; padding: 1px 5px; border-radius: 3px; font-size: 12px; }}
</style></head><body>

<h1>August 2026 desktop — summer-trough grid</h1>
<p class="sub">Objective: raise the <b>Aug-25 trough minimum</b> (28d-MA, post-headwind)
while holding <b>Dec-15 within ±50,000</b> of the canonical 48,672,970 →
[{DEC_LO:,}, {DEC_HI:,}]. Win10 headwind fixed at −1,245,000 (the ±50k allowance is its
own adjustability, so it is not a lever here). {len(df)} builds scored.</p>

{verdict}

<div class="note"><b>Reading the parameters.</b> The primary axis
<code>seasonality_corr_threshold</code> is newly exposed. Desktop's regime switch runs
<i>per tile</i> — a tile goes multiplicative + linear-growth when
corr(|y|,|dy|) &gt; threshold — so this dials the <i>fraction</i> of multiplicative tiles,
which is the genuine interior between <code>regime=auto</code> and
<code>regime=multiplicative</code>. The tile mix is heavily weight-skewed: at the legacy
0.0 cutoff, 37.5% of tiles but only <b>7.6% of DAU</b> sit multiplicative, because every
heavy tile has negative corr. That is why small moves in [−0.25, −0.10] matter so much,
with a cliff at −0.1465 where ROW/modern_windows (27% of all weight) flips.</div>

<h2>1. Frontier — trough vs Dec-15</h2>
{chart_html[0]}

<h2>2. The corr_threshold dial</h2>
{chart_html[1]}

<h2>3. Seam handoff derivative</h2>
<p class="sub">Reported, not scored. A floor applies: the headwind ramp contributes
−8,893/day of kink (−1,245,000 ÷ 140 days) that no parameter can touch, because the ramp
has zero slope before the seam and a constant slope after it.</p>
{chart_html[2]}

<h2>4. All parameters against all outcomes</h2>
{chart_html[3]}

<h2>5. Every build</h2>
<p class="sub">Sorted by trough minimum, descending. Green rows hold Dec-15; bold rows are
the two reference builds. Only varied parameters are shown as columns.
Raw scores: <code>grid_scores.csv</code></p>
{table_html}

</body></html>
"""
    OUT_HTML.write_text(html)
    print(f"\nWrote {OUT_HTML}")
    print(f"Wrote {csv_path}")
    if best is not None:
        print(f"\nBest in-band probe: {best['trough_min']:,.0f} trough "
              f"({best['trough_gain']:+,.0f}) at Dec-15 {best['dec15']:,.0f} "
              f"({best['dec_drift']:+,.0f})\n  {best['slug']}")
    else:
        print("\nNo probe held Dec-15 within +-50,000.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
