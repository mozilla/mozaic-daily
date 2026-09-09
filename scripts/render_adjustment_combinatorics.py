#!/usr/bin/env python3
"""Render the adjustment-combinatorics HTML report from the manifest that ``build_adjustment_combinatorics.py`` wrote.

For every subset of the droppable desktop overlays the report shows the canonical desktop
headline chart (actuals, the prior cycle's delivered curve, this subset's curve with the pinned
display layer applied, the prior-year actuals calendar-aligned, the Dec-15 targets, seam and
Dec-15 markers, DRAFT watermark) and a table of the Dec-15 28d-MA against August, against the
all-in September build and against the three targets. Sections are grouped by how many
adjustments were dropped.

Never runs the model. Reads the cached run parquets named in the manifest, the prior cycle's
delivered parquet, and one BigQuery query for desktop actuals (cached beside the report so a
re-render does not re-query).

Run (from the repo root):

    source .venv/bin/activate && python scripts/render_adjustment_combinatorics.py --cycle 2026-09

Cycle-scoped constants (prior build, targets, August figure) live in ``CYCLE_SETTINGS`` below;
add an entry at each roll-forward.
"""
from __future__ import annotations

import argparse
import base64
import html
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker as mticker  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from mozaic_daily.adjustments import (  # noqa: E402
    apply_net_adjustment_to_series, load_adjustments_from_dir, load_code_registry, load_forecast, render_adjustment,
)
from mozaic_daily.combinatorics import combination_rows, drop_groups, subset_label  # noqa: E402
from mozaic_daily.seam_ma import daily_to_28ma, display_ma  # noqa: E402

MANIFEST_NAME = "combinatorics_manifest.json"
REPORT_NAME = "index.html"
TABLE_CSV_NAME = "dec15_by_combination.csv"
ACTUALS_CACHE_NAME = "desktop_actuals_daily.parquet"
MEASUREMENT_DATE = pd.Timestamp("2026-12-15")
DISPLAY_START = pd.Timestamp("2026-01-01")
DISPLAY_END = pd.Timestamp("2026-12-31")
REFERENCE_YEAR = 2025
BQ_ACTUALS_START = "2025-12-04"   # 28 days before DISPLAY_START for the trailing MA

# Everything below that names another cycle's artifact or a pinned number. Mirrors the canonical
# notebook's [setup]; keep the two in step.
CYCLE_SETTINGS = {
    "2026-09": dict(
        prior_label="Prior (Aug 2026 delivered)",
        prior_short="August",
        current_label="Sep 2026",
        prior_parquet=(
            "data-official/2026-08/desktop_g01_2026-08-02/"
            "cps0.1649_thresh032_recent17_cpr0.814_ncp40_clip0.6_sps0.00825_regimemultiplicative/"
            "mozaic_daily_forecast.2026-08-02.ld-D.adj-lo.parquet"
        ),
        prior_required_state=["l", "o"],
        prior_adjustments_dir="data-official/2026-08/adjustments",
        prior_seam=pd.Timestamp("2026-08-02"),
        prior_delivered_dec15=48_703_443,
        targets={"Low": 49_039_852, "Baseline": 49_513_157, "Stretch": 49_772_388},
        show_draft_watermark=True,
        # Counterfactuals: all adjustments kept, the h anchor re-solved so the all-in Dec-15 lands at a chosen
        # delta vs the prior cycle. Display layer only, so exact and no model run.
        counterfactuals=[dict(key="h_for_plus479k", target_delta_vs_prior=479_000)],
    ),
}
HEADWIND_SPEC_NAME = "headwind.json"

WORLD_FILTER = dict(country="ALL", segment='{"os": "ALL"}', data_source="legacy_desktop", app_name="desktop")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cycle", required=True, help="cycle directory, e.g. 2026-09")
    parser.add_argument("--report-dir", type=Path, default=None,
                        help="default data-official/<cycle>/adjustment_combinatorics")
    parser.add_argument("--refresh-actuals", action="store_true", help="re-query BigQuery even if the actuals cache exists")
    return parser.parse_args()


# --- data -----------------------------------------------------------------------------------------

def load_world_daily(parquet: str | Path, require_state: list[str]) -> pd.DataFrame:
    """Validated load -> World(ALL) desktop daily DAU with data_type, sorted by date."""
    df, _ = load_forecast(REPO / parquet, require_state=require_state)
    mask = np.logical_and.reduce([df[column] == value for column, value in WORLD_FILTER.items()])
    world = df.loc[mask, ["target_date", "dau", "data_type"]].copy()
    world["target_date"] = pd.to_datetime(world["target_date"])
    return world.sort_values("target_date").reset_index(drop=True)


def fetch_desktop_actuals(cache: Path, refresh: bool) -> pd.DataFrame:
    """Daily desktop DAU actuals from BigQuery, cached to parquet beside the report."""
    if cache.exists() and not refresh:
        cached = pd.read_parquet(cache)
        print(f"actuals: cache {cache.name}, {len(cached)} rows through {cached['date'].max().date()}")
        return cached
    from google.cloud import bigquery  # imported here so a cached render needs no BigQuery client
    client = bigquery.Client(project="moz-fx-data-bq-data-science")
    sql = f"""
        SELECT submission_date AS date, SUM(dau) AS dau
        FROM `moz-fx-data-shared-prod.telemetry.active_users_aggregates`
        WHERE app_name = "Firefox Desktop"
          AND submission_date BETWEEN '{BQ_ACTUALS_START}' AND CURRENT_DATE("America/Los_Angeles") - 2
        GROUP BY submission_date ORDER BY 1
    """
    t0 = time.time()
    sys.stdout.write("actuals: querying BigQuery...")
    sys.stdout.flush()
    df = client.query(sql).to_dataframe()
    df["date"] = pd.to_datetime(df["date"])
    df["dau"] = df["dau"].astype(float)
    df.to_parquet(cache, index=False)
    sys.stdout.write(f" {len(df)} rows through {df['date'].max().date()} ({time.time() - t0:.1f}s), cached\n")
    return df


def prior_year_reference(world: pd.DataFrame) -> pd.Series:
    """Prior-year training rows as a 28d MA, calendar-shifted onto the display year (levels not rebased)."""
    training = world.loc[world["data_type"] == "training"]
    ma = daily_to_28ma(training["target_date"], training["dau"])
    reference = ma[ma.index.year == REFERENCE_YEAR].dropna()
    reference.index = reference.index + pd.DateOffset(years=DISPLAY_START.year - REFERENCE_YEAR)
    assert len(reference) == 365, f"{REFERENCE_YEAR} reference has {len(reference)} days, expected 365"
    return reference


def displayed_curve(world: pd.DataFrame, seam: pd.Timestamp, net_adjustments: dict) -> pd.Series:
    """Variance-matched display MA with the display layer summed on from the seam forward."""
    ma = display_ma(world["target_date"], world["dau"], seam)
    return apply_net_adjustment_to_series(ma, net_adjustments, "desktop", seam)


# --- plotting (mirrors the canonical notebook's helpers) ------------------------------------------

def millions_formatter(x, pos):
    return f"{x / 1e6:.2f}M"


def clip_display(s: pd.Series) -> pd.Series:
    return s[(s.index >= DISPLAY_START) & (s.index <= DISPLAY_END)].dropna()


def clip_forecast_only(s: pd.Series, seam: pd.Timestamp) -> pd.Series:
    return s[(s.index >= seam) & (s.index <= DISPLAY_END)].dropna()


def describe_subset(codes: list[str], names: dict[str, str]) -> str:
    return ", ".join(f"{code} ({names[code]})" for code in codes) if codes else "none"


def render_combination_plot(*, out_path: Path, label: str, included: list[str], dropped: list[str], names: dict[str, str],
                            actual_ma: pd.Series, prior_ma: pd.Series, current_ma: pd.Series, reference: pd.Series,
                            seam: pd.Timestamp, pinned_codes: list[str], settings: dict,
                            curve_note: str = "", title_note: str = "") -> None:
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(reference, label=f"{REFERENCE_YEAR} actuals (calendar-aligned)", color="grey", linewidth=0.9, alpha=0.55, zorder=1)
    ax.plot(clip_display(actual_ma), label="Actuals", color="black", linewidth=2)
    ax.plot(clip_display(prior_ma), label=settings["prior_label"], color="blue", linewidth=1)
    applied = "+".join(sorted(pinned_codes + included))
    shown = clip_forecast_only(current_ma, seam)
    ax.plot(shown, label=f"Current ({settings['current_label']}, adj-{applied}{curve_note})  Dec-15 {shown.get(MEASUREMENT_DATE):,.0f}",
            color="green", linewidth=1.4)
    for (target_name, value), marker in zip(settings["targets"].items(), ["^", "v", "D"]):
        ax.plot(MEASUREMENT_DATE, value, marker=marker, color="gold", markersize=12, markeredgecolor="black",
                markeredgewidth=0.8, linestyle="None", label=f"Target {target_name} ({value:,})")
    ax.axvline(MEASUREMENT_DATE, color="red", linestyle=":", alpha=0.5, label="Dec 15")
    ax.axvline(seam, color="grey", linestyle="--", alpha=0.6, label=f"Seam {seam.date()}")
    dropped_text = title_note or (f"dropped: {'+'.join(dropped)}" if dropped else "all adjustments in")
    ax.set_title(f"2026 Desktop DAU — 28-Day MA (vs {settings['prior_short']} delivered, with {REFERENCE_YEAR} actuals) — "
                 f"adjustments {applied} ({dropped_text})", fontsize=13)
    ax.set_ylabel("DAU")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(millions_formatter))
    ax.legend(loc="lower left", fontsize=9)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    ax.grid(True, alpha=0.3)
    if settings["show_draft_watermark"]:
        ax.text(0.5, 0.5, "DRAFT", transform=ax.transAxes, fontsize=120, color="#c0392b", alpha=0.16,
                ha="center", va="center", rotation=28, zorder=100, fontweight="bold", clip_on=True)
    plt.tight_layout()
    fig.savefig(out_path, dpi=130)
    plt.close(fig)


def render_overview_plot(*, out_path: Path, curves: dict[str, pd.Series], actual_ma: pd.Series, prior_ma: pd.Series,
                         seam: pd.Timestamp, settings: dict) -> None:
    """Every combination on one chart, coloured by how many adjustments were dropped."""
    fig, ax = plt.subplots(figsize=(14, 7))
    ax.plot(clip_display(actual_ma), label="Actuals", color="black", linewidth=2)
    ax.plot(clip_display(prior_ma), label=settings["prior_label"], color="blue", linewidth=1.6)
    max_dropped = max(len(label.split("+")) for label in curves) if curves else 0
    palette = plt.cm.viridis(np.linspace(0.1, 0.9, max_dropped + 1))
    for label, curve in curves.items():
        n_in = 0 if label == "raw" else len(label.split("+"))
        n_dropped = max_dropped - n_in
        shown = clip_forecast_only(curve, seam)
        ax.plot(shown, color=palette[n_dropped], linewidth=1.0, alpha=0.9,
                label=f"{label:<8s} Dec-15 {shown.get(MEASUREMENT_DATE):,.0f}")
    for (target_name, value), marker in zip(settings["targets"].items(), ["^", "v", "D"]):
        ax.plot(MEASUREMENT_DATE, value, marker=marker, color="gold", markersize=12, markeredgecolor="black",
                markeredgewidth=0.8, linestyle="None", label=f"Target {target_name} ({value:,})")
    ax.axvline(MEASUREMENT_DATE, color="red", linestyle=":", alpha=0.5)
    ax.axvline(seam, color="grey", linestyle="--", alpha=0.6)
    ax.set_xlim(pd.Timestamp("2026-07-01"), DISPLAY_END)
    ax.set_title("2026 Desktop DAU — 28-Day MA: every adjustment combination (h always applied; darker = more dropped)", fontsize=13)
    ax.set_ylabel("DAU")
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(millions_formatter))
    ax.legend(loc="upper left", fontsize=7.5, ncol=2, prop={"family": "monospace", "size": 7.5})
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
    ax.grid(True, alpha=0.3)
    if settings["show_draft_watermark"]:
        ax.text(0.5, 0.5, "DRAFT", transform=ax.transAxes, fontsize=120, color="#c0392b", alpha=0.16,
                ha="center", va="center", rotation=28, zorder=100, fontweight="bold", clip_on=True)
    plt.tight_layout()
    fig.savefig(out_path, dpi=130)
    plt.close(fig)


# --- HTML -------------------------------------------------------------------------------------------

STYLE = """
body { font-family: -apple-system, Helvetica, Arial, sans-serif; margin: 2rem auto; max-width: 1400px; color: #222; }
h1 { border-bottom: 2px solid #444; padding-bottom: .3rem; }
h2 { margin-top: 3rem; border-bottom: 1px solid #bbb; }
h3 { margin-top: 2.5rem; }
table { border-collapse: collapse; margin: 1rem 0; font-size: 14px; }
th, td { border: 1px solid #ccc; padding: 4px 10px; text-align: right; white-space: nowrap; }
th { background: #f0f0f0; } td.txt, th.txt { text-align: left; }
tr.full { background: #e8f5e9; } tr.raw { background: #fbe9e7; }
.pos { color: #1b5e20; } .neg { color: #b71c1c; }
img { max-width: 100%; border: 1px solid #ddd; }
.meta { font-size: 13px; color: #555; } code { background: #f4f4f4; padding: 1px 4px; }
.toc a { margin-right: 1.2rem; }
pre.copyable { background: #f7f7f7; border: 1px solid #ccc; padding: .8rem; font-size: 12.5px; overflow-x: auto; }
button.copy { font-size: 13px; padding: 3px 10px; margin-bottom: .3rem; }
"""


def signed(value: float) -> str:
    cls = "pos" if value >= 0 else "neg"
    return f'<td class="{cls}">{value:+,.0f}</td>'


def table_header(target_names: list[str]) -> str:
    cells = ["adjustments in", "dropped", "# dropped", "Dec-15 28d-MA", "vs August", "vs August %", "vs Sep all-in"]
    cells += [f"vs {t}" for t in target_names]
    return "<tr>" + "".join(f'<th class="txt">{html.escape(c)}</th>' if i < 2 else f"<th>{html.escape(c)}</th>"
                             for i, c in enumerate(cells)) + "</tr>"


def table_row(row: dict, pinned: list[str], target_names: list[str], link: bool) -> str:
    applied = "+".join(sorted(pinned + row["included"]))
    name = f'<a href="#{row["label"]}">{applied}</a>' if link else applied
    css = "full" if row["n_dropped"] == 0 else ("raw" if not row["included"] else "")
    cells = [f'<td class="txt">{name}</td>',
             f'<td class="txt">{"+".join(row["dropped"]) or "—"}</td>',
             f'<td>{row["n_dropped"]}</td>',
             f'<td><b>{row["dec15"]:,.0f}</b></td>',
             signed(row["delta_vs_prior"]),
             f'<td class="{"pos" if row["pct_vs_prior"] >= 0 else "neg"}">{row["pct_vs_prior"]:+.2f}%</td>',
             signed(row["delta_vs_full"])]
    cells += [signed(row[f"delta_vs_target_{t.lower()}"]) for t in target_names]
    return f'<tr class="{css}">' + "".join(cells) + "</tr>"


def embed_png(path: Path) -> str:
    return "data:image/png;base64," + base64.b64encode(path.read_bytes()).decode()


def plain_text_table(rows: list[dict], pinned: list[str], target_names: list[str]) -> str:
    """The Dec-15 table as an aligned Markdown pipe table, for pasting into a text document."""
    header = ["adjustments in", "dropped", "# dropped", "Dec-15 28d-MA", "vs August", "vs August %", "vs Sep all-in"]
    header += [f"vs {t}" for t in target_names]
    lines = []
    for row in rows:
        cells = ["+".join(sorted(pinned + row["included"])), "+".join(row["dropped"]) or "-", str(row["n_dropped"]),
                 f"{row['dec15']:,.0f}", f"{row['delta_vs_prior']:+,.0f}", f"{row['pct_vs_prior']:+.2f}%",
                 f"{row['delta_vs_full']:+,.0f}"]
        cells += [f"{row[f'delta_vs_target_{t.lower()}']:+,.0f}" for t in target_names]
        lines.append(cells)
    widths = [max(len(h), *(len(line[i]) for line in lines)) for i, h in enumerate(header)]
    left = {0, 1}   # text columns left-aligned, numbers right-aligned

    def fmt(cells):
        return "| " + " | ".join(c.ljust(w) if i in left else c.rjust(w) for i, (c, w) in enumerate(zip(cells, widths))) + " |"

    rule = "|" + "|".join((":" + "-" * (w + 1)) if i in left else ("-" * (w + 1) + ":") for i, w in enumerate(widths)) + "|"
    return "\n".join([fmt(header), rule] + [fmt(line) for line in lines])


COPY_SCRIPT = """
<script>
function copyPre(id, btn) {
  navigator.clipboard.writeText(document.getElementById(id).textContent).then(() => {
    const was = btn.textContent; btn.textContent = 'copied'; setTimeout(() => btn.textContent = was, 1200);
  });
}
</script>
"""


def solve_headwind_counterfactual(*, spec_path: Path, net_adjustments: dict, date_index: pd.DatetimeIndex,
                                  full_world: pd.DataFrame, seam: pd.Timestamp, prior_dec15: float,
                                  target_delta_vs_prior: float) -> dict:
    """Re-anchor the linear-ramp headwind so the all-in curve lands at ``prior + target`` on Dec-15.

    The ramp's Dec-15 value is its anchor (clamped or not, Dec-15 is the anchor date), so the new anchor
    is the old one shifted by exactly the Dec-15 gap. Verified by re-rendering, not assumed.
    """
    spec = json.loads(spec_path.read_text())
    assert spec["type"] == "linear_ramp" and pd.Timestamp(spec["anchor_date"]) == MEASUREMENT_DATE, \
        f"{spec_path.name} is not a linear_ramp anchored at Dec-15; the counterfactual solve assumes that"
    rendered_h = render_adjustment(spec, date_index, spec_dir=spec_path.parent)["desktop"]
    baseline = displayed_curve(full_world, seam, net_adjustments)
    baseline_dec15 = float(baseline.get(MEASUREMENT_DATE))
    target_dec15 = prior_dec15 + target_delta_vs_prior
    new_anchor = spec["desktop_dau"] + (target_dec15 - baseline_dec15)
    cf_spec = {**spec, "desktop_dau": new_anchor}
    rendered_cf = render_adjustment(cf_spec, date_index, spec_dir=spec_path.parent)["desktop"]
    cf_net = {"desktop": net_adjustments["desktop"] - rendered_h + rendered_cf, "mobile": net_adjustments["mobile"]}
    curve = displayed_curve(full_world, seam, cf_net)
    achieved = float(curve.get(MEASUREMENT_DATE)) - prior_dec15
    assert abs(achieved - target_delta_vs_prior) < 1, f"counterfactual missed: {achieved:+,.0f} vs {target_delta_vs_prior:+,.0f}"
    return dict(curve=curve, original_anchor=spec["desktop_dau"], new_anchor=new_anchor, baseline_dec15=baseline_dec15,
                dec15=float(curve.get(MEASUREMENT_DATE)), target_delta_vs_prior=target_delta_vs_prior)


def counterfactual_rows(counterfactuals: list[dict], full_row: dict, prior_dec15: float, targets: dict) -> list[dict]:
    """Table rows for the counterfactual section: the all-in build, then each re-anchored variant."""
    rows = [{**full_row, "label": "all-in", "h_anchor": counterfactuals[0]["original_anchor"] if counterfactuals else None}]
    for cf in counterfactuals:
        row = {"label": cf["key"], "included": full_row["included"], "dropped": [], "n_dropped": 0, "h_anchor": cf["new_anchor"],
               "dec15": cf["dec15"], "delta_vs_prior": cf["dec15"] - prior_dec15,
               "pct_vs_prior": (cf["dec15"] / prior_dec15 - 1) * 100, "delta_vs_full": cf["dec15"] - full_row["dec15"]}
        for name, value in targets.items():
            row[f"delta_vs_target_{name.lower()}"] = cf["dec15"] - value
        rows.append(row)
    return rows


def counterfactual_table_html(rows: list[dict], target_names: list[str]) -> str:
    cells = ["variant", "h anchor at Dec-15", "Dec-15 28d-MA", "vs August", "vs August %", "vs Sep all-in"] + [f"vs {t}" for t in target_names]
    out = "<table><tr>" + "".join(f'<th class="txt">{c}</th>' if i == 0 else f"<th>{c}</th>" for i, c in enumerate(cells)) + "</tr>"
    for row in rows:
        out += ("<tr>" + f'<td class="txt">{html.escape(row["label"])}</td>' + f'<td>{row["h_anchor"]:+,.0f}</td>'
                + f'<td><b>{row["dec15"]:,.0f}</b></td>' + signed(row["delta_vs_prior"])
                + f'<td class="{"pos" if row["pct_vs_prior"] >= 0 else "neg"}">{row["pct_vs_prior"]:+.2f}%</td>'
                + signed(row["delta_vs_full"]) + "".join(signed(row[f"delta_vs_target_{t.lower()}"]) for t in target_names) + "</tr>")
    return out + "</table>"


def counterfactual_table_text(rows: list[dict], target_names: list[str]) -> str:
    header = ["variant", "h anchor at Dec-15", "Dec-15 28d-MA", "vs August", "vs August %", "vs Sep all-in"] + [f"vs {t}" for t in target_names]
    lines = [[row["label"], f"{row['h_anchor']:+,.0f}", f"{row['dec15']:,.0f}", f"{row['delta_vs_prior']:+,.0f}",
              f"{row['pct_vs_prior']:+.2f}%", f"{row['delta_vs_full']:+,.0f}"]
             + [f"{row[f'delta_vs_target_{t.lower()}']:+,.0f}" for t in target_names] for row in rows]
    widths = [max(len(h), *(len(line[i]) for line in lines)) for i, h in enumerate(header)]

    def fmt(cells):
        return "| " + " | ".join(c.ljust(w) if i == 0 else c.rjust(w) for i, (c, w) in enumerate(zip(cells, widths))) + " |"

    rule = "|" + "|".join((":" + "-" * (w + 1)) if i == 0 else ("-" * (w + 1) + ":") for i, w in enumerate(widths)) + "|"
    return "\n".join([fmt(header), rule] + [fmt(line) for line in lines])


def build_html(*, manifest: dict, rows: list[dict], names: dict[str, str], settings: dict, pinned: list[str],
               plots: dict[str, Path], overview: Path, actuals_through: pd.Timestamp, full_dec15: float,
               counterfactuals: list[dict], cf_rows: list[dict], cf_plots: dict[str, Path]) -> str:
    target_names = list(settings["targets"])
    droppable = manifest["droppable_codes"]
    parts = [f"<!doctype html><html><head><meta charset='utf-8'><title>Adjustment combinatorics — {manifest['cycle']} desktop</title>",
             f"<style>{STYLE}</style></head><body>",
             f"<h1>Desktop adjustment combinatorics — {manifest['cycle']} cycle, seam {manifest['forecast_start']}</h1>"]
    if settings["show_draft_watermark"]:
        parts.append("<p><b style='color:#c0392b'>DRAFT</b> — no combination here is locked; every chart is watermarked until the September forecast is locked.</p>")
    parts.append(
        "<p>Question: not every adjustment can ship this cycle, so which subset should? "
        f"The Win10 headwind <code>h</code> ({manifest['display_effects_dec15'].get('h', 0):+,.0f} at Dec-15, display layer, exact) is applied in every row. "
        f"Every subset of the {len(droppable)} droppable per-tile overlays is a real desktop model run with exactly that subset "
        "subtracted from training rows before mozaic and added back after, so interactions through the Prophet fit are measured, not assumed. "
        f"Dec-15 values are the World 28-day trailing MA; deltas are against {settings['prior_short']}'s delivered "
        f"<b>{settings['prior_delivered_dec15']:,}</b> and against this cycle's all-in build <b>{full_dec15:,.0f}</b>.</p>")
    parts.append("<ul class='meta'>")
    parts.append(f"<li>Droppable overlays: {html.escape(describe_subset(droppable, names))}</li>")
    parts.append(f"<li>Always applied: {html.escape(describe_subset(pinned, names))}</li>")
    parts.append(f"<li>Model config: <code>{html.escape(json.dumps(manifest['model_config'], sort_keys=True))}</code></li>")
    parts.append(f"<li>Runs built {manifest['built_at']}; report rendered {datetime.now(timezone.utc).isoformat(timespec='seconds')}; "
                 f"actuals through {actuals_through.date()}.</li>")
    parts.append("<li>Caveat for the rows that drop <code>l</code> or <code>o</code>: both were already in August's published build, so those rows remove something the prior cycle delivered rather than declining a new addition.</li>")
    parts.append("</ul>")

    parts.append("<h2 id='overview'>All combinations</h2>")
    parts.append(f"<img src='{embed_png(overview)}' alt='all combinations'>")
    parts.append("<h2 id='table'>Dec-15 by combination</h2>")
    parts.append("<table>" + table_header(target_names) + "".join(table_row(r, pinned, target_names, True) for r in rows) + "</table>")
    parts.append("<p class='meta'>Green row: all adjustments in (the current draft canonical). Red row: h only, every droppable overlay out. "
                 "Rows sorted by number dropped, then alphabetically. Click a row to jump to its chart.</p>")
    parts.append("<h3>Same table as plain text (for pasting into a document)</h3>")
    parts.append("<button class='copy' onclick=\"copyPre('plain-table', this)\">copy to clipboard</button>")
    parts.append(f"<pre class='copyable' id='plain-table'>{html.escape(plain_text_table(rows, pinned, target_names))}</pre>")
    parts.append(COPY_SCRIPT)

    if counterfactuals:
        parts.append("<h2 id='counterfactual'>Counterfactual: all five adjustments, h re-anchored to a chosen delta vs August</h2>")
        for cf in counterfactuals:
            parts.append(
                f"<p>All of <code>i j l o</code> stay in; the Win10 headwind anchor is re-solved so the all-in Dec-15 lands exactly "
                f"<b>{cf['target_delta_vs_prior']:+,}</b> above {settings['prior_short']}'s delivered {settings['prior_delivered_dec15']:,}. "
                f"That requires an anchor of <b>{cf['new_anchor']:+,.0f}</b> instead of the draft {cf['original_anchor']:+,.0f}: the headwind gets "
                f"<b>{abs(cf['new_anchor'] - cf['original_anchor']):,.0f} larger</b> (more negative), not smaller — i.e. it gives back that much of the "
                f"+{cf['original_anchor'] - (-1_315_000):,.0f} relief the September re-anchor introduced versus August's −1,315,000. "
                "Display layer only, so this is exact at Dec-15 and needed no model run; the ramp still starts at 0 on the seam and is flat after Dec-15.</p>")
        parts.append(counterfactual_table_html(cf_rows, target_names))
        parts.append("<h3>Same table as plain text</h3>")
        parts.append("<button class='copy' onclick=\"copyPre('plain-cf-table', this)\">copy to clipboard</button>")
        parts.append(f"<pre class='copyable' id='plain-cf-table'>{html.escape(counterfactual_table_text(cf_rows, target_names))}</pre>")
        for cf in counterfactuals:
            parts.append(f"<img src='{embed_png(cf_plots[cf['key']])}' alt='{html.escape(cf['key'])}'>")
            parts.append(f"<p class='meta'>plot file: <code>{cf_plots[cf['key']].relative_to(REPO)}</code></p>")

    groups = drop_groups(rows)
    parts.append("<p class='toc'>" + "".join(f"<a href='#drop-{n}'>drop {n}</a>" for n in groups) + "</p>")
    for n_dropped, group in groups.items():
        title = {0: "Drop none — all adjustments in", len(droppable): f"Drop all {len(droppable)} — h only"}.get(
            n_dropped, f"Drop {n_dropped}")
        parts.append(f"<h2 id='drop-{n_dropped}'>{title}</h2>")
        for row in group:
            applied = "+".join(sorted(pinned + row["included"]))
            parts.append(f"<h3 id='{row['label']}'>adj-{applied} — dropped: {'+'.join(row['dropped']) or 'nothing'}</h3>")
            parts.append(f"<p class='meta'>In: {html.escape(describe_subset(sorted(pinned + row['included']), names))}. "
                         f"Out: {html.escape(describe_subset(row['dropped'], names))}.</p>")
            parts.append("<table>" + table_header(target_names) + table_row(row, pinned, target_names, False) + "</table>")
            parts.append(f"<img src='{embed_png(plots[row['label']])}' alt='{html.escape(applied)}'>")
            parts.append(f"<p class='meta'>plot file: <code>{plots[row['label']].relative_to(REPO)}</code></p>")
    parts.append("</body></html>")
    return "\n".join(parts)


# --- main -------------------------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    if args.cycle not in CYCLE_SETTINGS:
        raise SystemExit(f"no CYCLE_SETTINGS entry for {args.cycle}; add one (prior build, targets, delivered figure)")
    settings = CYCLE_SETTINGS[args.cycle]
    cycle_dir = REPO / "data-official" / args.cycle
    report_dir = args.report_dir or (cycle_dir / "adjustment_combinatorics")
    plots_dir = report_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)
    manifest = json.loads((report_dir / MANIFEST_NAME).read_text())
    seam = pd.Timestamp(manifest["forecast_start"])
    assert str(MEASUREMENT_DATE.date()) == manifest["measurement_date"], "manifest measurement date != Dec-15"
    # Codes shown as "always applied" in labels: the display layer (h) plus any pinned overlays.
    pinned = sorted(manifest["display_effects_dec15"]) + list(manifest.get("pinned_overlay_codes", []))
    names = {code: entry["name"] for code, entry in load_code_registry().items()}

    # Display layer: the current cycle's specs for every combination, the prior cycle's own specs for the prior line.
    date_index = pd.date_range("2025-01-01", "2027-12-31", freq="D")
    net_adjustments = load_adjustments_from_dir(cycle_dir / "adjustments", date_index, require_specs=True)
    prior_net_adjustments = load_adjustments_from_dir(REPO / settings["prior_adjustments_dir"], date_index, require_specs=True)
    display_total = float(net_adjustments["desktop"].get(MEASUREMENT_DATE))
    manifest_display_total = sum(manifest["display_effects_dec15"].values())
    if abs(display_total - manifest_display_total) > 0.5:
        print(f"WARNING: display-layer Dec-15 is now {display_total:+,.0f} but the manifest was built with {manifest_display_total:+,.0f}; "
              "the table uses the CURRENT specs.")

    actuals = fetch_desktop_actuals(report_dir / ACTUALS_CACHE_NAME, args.refresh_actuals)
    actual_ma = daily_to_28ma(actuals["date"], actuals["dau"])
    prior_world = load_world_daily(settings["prior_parquet"], settings["prior_required_state"])
    prior_ma = displayed_curve(prior_world, settings["prior_seam"], prior_net_adjustments)
    prior_dec15 = float(prior_ma.get(MEASUREMENT_DATE))
    drift = prior_dec15 - settings["prior_delivered_dec15"]
    assert abs(drift) < 1000, f"{settings['prior_short']} delivered Dec-15 does not reproduce (drift {drift:+,.0f}); do not quote deltas"
    print(f"prior reproduces {settings['prior_short']} delivered: {prior_dec15:,.0f} (drift {drift:+,.0f})")

    curves: dict[str, pd.Series] = {}
    run_dec15: dict[frozenset[str], float] = {}
    reference = None
    for label, run in manifest["runs"].items():
        world = load_world_daily(run["parquet"], run["overlays_enabled"])
        if reference is None:
            reference = prior_year_reference(world)   # training rows are raw actuals in every run
        curve = displayed_curve(world, seam, net_adjustments)
        plotted = float(curve.get(MEASUREMENT_DATE))
        expected = run["dec15_plain_ma"] + display_total
        assert abs(plotted - expected) < 1, f"{label}: plotted Dec-15 {plotted:,.0f} != manifest {expected:,.0f}"
        curves[label] = curve
        run_dec15[frozenset(run["overlay_subset"])] = run["dec15_plain_ma"]
        print(f"  {label:<10s} Dec-15 {plotted:>14,.0f}")

    full_label = subset_label(manifest["droppable_codes"])
    full_dec15 = float(curves[full_label].get(MEASUREMENT_DATE))
    rows = combination_rows(all_codes=manifest["droppable_codes"], run_dec15=run_dec15, display_total_dec15=display_total,
                            prior_dec15=prior_dec15, full_dec15=full_dec15, targets=settings["targets"])

    plots: dict[str, Path] = {}
    for row in rows:
        out_path = plots_dir / f"desktop_adj-{'+'.join(sorted(pinned + row['included']))}.png"
        render_combination_plot(out_path=out_path, label=row["label"], included=row["included"], dropped=row["dropped"],
                                names=names, actual_ma=actual_ma, prior_ma=prior_ma, current_ma=curves[row["label"]],
                                reference=reference, seam=seam, pinned_codes=pinned, settings=settings)
        plots[row["label"]] = out_path
    overview = plots_dir / "desktop_all_combinations.png"
    render_overview_plot(out_path=overview, curves=curves, actual_ma=actual_ma, prior_ma=prior_ma, seam=seam, settings=settings)
    print(f"plots: {len(plots) + 1} PNGs in {plots_dir.relative_to(REPO)}")

    full_world = load_world_daily(manifest["runs"][full_label]["parquet"], manifest["runs"][full_label]["overlays_enabled"])
    counterfactuals, cf_plots = [], {}
    for cf_setting in settings.get("counterfactuals", []):
        cf = solve_headwind_counterfactual(spec_path=cycle_dir / "adjustments" / HEADWIND_SPEC_NAME, net_adjustments=net_adjustments,
                                           date_index=date_index, full_world=full_world, seam=seam, prior_dec15=prior_dec15,
                                           target_delta_vs_prior=cf_setting["target_delta_vs_prior"])
        cf["key"] = cf_setting["key"]
        counterfactuals.append(cf)
        out_path = plots_dir / f"desktop_counterfactual_{cf['key']}.png"
        render_combination_plot(out_path=out_path, label=cf["key"], included=manifest["droppable_codes"], dropped=[], names=names,
                                actual_ma=actual_ma, prior_ma=prior_ma, current_ma=cf["curve"], reference=reference, seam=seam,
                                pinned_codes=pinned, settings=settings, curve_note=f", h anchor {cf['new_anchor']:+,.0f}",
                                title_note=f"counterfactual, h {cf['new_anchor']:+,.0f}")
        cf_plots[cf["key"]] = out_path
        print(f"counterfactual {cf['key']}: h {cf['original_anchor']:+,.0f} -> {cf['new_anchor']:+,.0f}, Dec-15 {cf['dec15']:,.0f} "
              f"({cf['dec15'] - prior_dec15:+,.0f} vs {settings['prior_short']})")
    full_row = next(r for r in rows if r["n_dropped"] == 0)
    cf_rows = counterfactual_rows(counterfactuals, full_row, prior_dec15, settings["targets"]) if counterfactuals else []
    if cf_rows:
        cf_table = pd.DataFrame(cf_rows).drop(columns=["included", "dropped", "n_dropped"])
        cf_table.to_csv(report_dir / "counterfactuals.csv", index=False)

    table = pd.DataFrame(rows)
    table["included"] = table["included"].map("+".join)
    table["dropped"] = table["dropped"].map("+".join)
    table.insert(0, "adjustments_applied", [("+".join(sorted(pinned + r["included"]))) for r in rows])
    table.to_csv(report_dir / TABLE_CSV_NAME, index=False)

    report = build_html(manifest=manifest, rows=rows, names=names, settings=settings, pinned=pinned, plots=plots,
                        overview=overview, actuals_through=actuals["date"].max(), full_dec15=full_dec15,
                        counterfactuals=counterfactuals, cf_rows=cf_rows, cf_plots=cf_plots)
    (report_dir / REPORT_NAME).write_text(report)
    print(f"wrote {report_dir.relative_to(REPO) / REPORT_NAME} and {TABLE_CSV_NAME}")
    print(f"\n{'adjustments':<12s} {'dropped':<8s} {'Dec-15':>14s} {'vs Aug':>12s} {'vs all-in':>12s}")
    for row in rows:
        print(f"{'+'.join(sorted(pinned + row['included'])):<12s} {'+'.join(row['dropped']) or '-':<8s} "
              f"{row['dec15']:>14,.0f} {row['delta_vs_prior']:>+12,.0f} {row['delta_vs_full']:>+12,.0f}")


if __name__ == "__main__":
    main()
