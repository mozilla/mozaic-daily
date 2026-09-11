"""Write half of the paid-DAU curve pull: parquet + csv twin + meta + workbook + plot + hand-off note.

Everything here lands under `data-official/{cycle}/marketing/` with a **pull-date suffix**, so a
re-pull within a cycle is a sibling of the previous build, never an overwrite. Nothing the forecast
reads (`organic.json`, tests, registry) is touched: the `PENDING_WIRING.md` note is the hand-off to
whoever wires the curve, and that agent deletes it when done.

The rows may come from the query (`PullProvenance`) or a delivered workbook
(`DeliveredFileProvenance`); see `paid_curve_provenance`. The files written are the same either way.
"""
from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Union

import pandas as pd

from mozaic_daily.paid_curve import basis_with_variant, curve_stem, daily_type_labels
from mozaic_daily.paid_curve_provenance import (  # noqa: F401  (re-exported for the script)
    DeliveredFileProvenance, PullProvenance, relative_to_repo, sha1_of,
)

PENDING_NOTE = "PENDING_WIRING.md"
Provenance = Union[PullProvenance, DeliveredFileProvenance]


def write_curve_files(out_dir: Path, basis: str, forecast_start: str, provenance: Provenance,
                      weekly: pd.DataFrame, daily: pd.DataFrame, values: dict, repo: Path,
                      raw_frame: pd.DataFrame) -> dict[str, Path]:
    """Write parquet, csv twin, workbook, plot and meta; return the paths by role.

    `raw_frame` is what came in, as delivered (the query rows, or the delivered sheet), for the
    workbook's first tab.
    """
    stem = curve_stem(basis, forecast_start, provenance.pull_date, provenance.variant)
    side_stem = "paid_dau_curve" + (f".{provenance.variant}" if provenance.variant else "")
    paths = {
        "parquet": out_dir / f"{stem}.parquet",
        "csv": out_dir / f"{stem}.csv",
        "meta": out_dir / f"{stem}.meta.json",
        "workbook": out_dir / f"{side_stem}.{forecast_start}.pull{provenance.pull_date}.xlsx",
        "plot": out_dir / "plots" / f"{side_stem}.{forecast_start}.pull{provenance.pull_date}.png",
    }
    for path in paths.values():
        if path.exists():
            raise FileExistsError(f"{path} exists; a pull is never overwritten. Pass a different --pull-date "
                                  "or remove the earlier sibling deliberately.")
    paths["plot"].parent.mkdir(parents=True, exist_ok=True)

    daily.to_parquet(paths["parquet"])
    csv_twin = daily.assign(type=daily_type_labels(daily, weekly)).reset_index()
    csv_twin["target_date"] = csv_twin["target_date"].dt.date
    csv_twin.to_csv(paths["csv"], index=False)
    write_workbook(paths["workbook"], raw_frame, provenance.raw_sheet_name, weekly, csv_twin)
    write_plot(paths["plot"], weekly, daily, forecast_start, provenance.plot_subtitle, provenance.variant)
    paths["meta"].write_text(json.dumps(
        build_meta(basis, forecast_start, provenance, weekly, daily, values, paths, repo),
        indent=2) + "\n")
    return paths


def build_meta(basis: str, forecast_start: str, provenance: Provenance, weekly: pd.DataFrame,
               daily: pd.DataFrame, values: dict, paths: dict, repo: Path) -> dict:
    last_actual_week = weekly.loc[weekly["is_actual"], "date"].max()
    git_hash = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True).stdout.strip()
    return {
        "model_name": f"fenix_paid_dau_level_gmio_uac_meta_{basis_with_variant(basis, provenance.variant)}",
        "variant": provenance.variant or "point_estimate",
        "description": (f"Paid-DAU level for the `p` paid/organic split, from {provenance.origin} "
                        "(UAC + Meta Android, Meta stacked cumulatively), one value per week interpolated to daily and "
                        "written as delivered: `p` stacks paid_dau_level_daily verbatim. "
                        "Produced by scripts/pull_paid_dau_curve.py, not wired."),
        "wiring_status": "pending",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "mozaic_daily_git_hash": git_hash,
        "forecast_start_date": forecast_start,
        "coverage": {"start_date": str(daily.index.min().date()), "end_date": str(daily.index.max().date()),
                     "actuals_through_week_of": str(last_actual_week.date()),
                     "last_weekly_row": str(weekly["date"].max().date()),
                     "tail_rule": f"forward-fill after the last Monday to {daily.index.max().date()}; `p` holds flat past that"},
        "methodology": {"framing": "level as delivered (the lift-plus-anchor round-trip was retired 2026-09-09)",
                        "composition": provenance.composition,
                        "weekly_to_daily": "value on its ISO Monday, linear interpolation, forward-fill after the last Monday",
                        "metric_basis": basis, "variant": provenance.variant or "point_estimate",
                        "meta_channel": "included, stacked cumulatively, assumed fully incremental",
                        "iran": "not in the feed, so ex-IR by construction"},
        "source_data": {**provenance.source_section(repo), "weekly_rows": int(len(weekly)),
                        "basis_counts": {k: int(v) for k, v in weekly["basis"].value_counts().items()}},
        "key_values": values,
        "known_limitations": [
            "The forecast reflects the marketing spend plan and the GMIO refit; a change versus the previous pull is mostly plan, not actuals revision.",
            "Weekly values are treated as the level on their Monday and interpolated; within-week shape is not observed.",
            "Curve ends Dec 31 of the forecast year; `p` holds it flat through the following year by its tail_policy.",
            "Not wired: organic.json must point paid_forecast.data_file at this parquet with value_column paid_dau_level_daily (see PENDING_WIRING.md).",
        ],
        "artifact_sha1": sha1_of(paths["parquet"]),
    }


def write_workbook(path: Path, raw_frame: pd.DataFrame, raw_sheet_name: str, weekly: pd.DataFrame,
                   daily_csv_twin: pd.DataFrame) -> None:
    with pd.ExcelWriter(path, engine="openpyxl") as book:
        raw_frame.to_excel(book, sheet_name=raw_sheet_name, index=False)
        weekly.assign(date=weekly["date"].dt.date).to_excel(book, sheet_name="composed_weekly", index=False)
        daily_csv_twin.to_excel(book, sheet_name="daily", index=False)


def write_plot(path: Path, weekly: pd.DataFrame, daily: pd.DataFrame, forecast_start: str,
               subtitle: str, variant: str | None = None) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    kpi = pd.Timestamp(year=pd.Timestamp(forecast_start).year, month=12, day=15)
    fig, ax = plt.subplots(figsize=(12, 5), facecolor="#fcfcfb")
    ax.set_facecolor("#fcfcfb")
    ax.grid(alpha=0.25)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e6:.2f}M"))
    ax.plot(daily.index, daily["paid_dau_level_daily"], color="#2a78d6", lw=1.6, label="daily level (interpolated)")
    ax.plot(daily.index, daily["paid_dau_level_ma"], color="#2a78d6", lw=1.0, ls="--", label="28d MA")
    actual, forecast = weekly[weekly["is_actual"]], weekly[~weekly["is_actual"]]
    ax.scatter(actual["date"], actual["paid_dau_used"], color="#2a78d6", s=18, zorder=3, label="weekly, actual")
    ax.scatter(forecast["date"], forecast["paid_dau_used"], facecolors="none", edgecolors="#2a78d6", s=22, zorder=3, label="weekly, forecast")
    ax.set_ylabel("paid DAU level")
    ax.legend(loc="upper left", fontsize=9, frameon=False)
    variant_label = f" [{variant}]" if variant else ""
    ax.set_title(f"Paid-DAU level for `p`{variant_label}, seam {forecast_start} — {subtitle}", loc="left")
    for when, text in ((pd.Timestamp(forecast_start), "seam"), (kpi, "Dec-15")):
        ax.axvline(when, color="#52514e", ls=":", lw=1)
        ax.text(when, 0.02, f" {text}", transform=ax.get_xaxis_transform(), fontsize=9, color="#52514e")
    fig.tight_layout()
    fig.savefig(path, dpi=130, facecolor="#fcfcfb")
    plt.close(fig)


def write_pending_note(out_dir: Path, paths: dict[str, Path], values: dict, provenance: Provenance,
                       forecast_start: str, currently_wired: str | None, repo: Path) -> Path:
    """The hand-off flag for the wiring step. Overwritten on each pull; deleted by whoever wires the curve."""
    note = out_dir / PENDING_NOTE
    lines = [
        f"# Pending wiring — paid-DAU curve pulled {provenance.pull_date}",
        "",
        "Written by `scripts/pull_paid_dau_curve.py`. This directory holds a **new, unwired** paid curve. Nothing the",
        "forecast reads was changed. Wiring is a separate step; delete this file once it is done.",
        "",
        f"- new curve: `{relative_to_repo(paths['parquet'], repo)}`",
        f"- its meta: `{relative_to_repo(paths['meta'], repo)}`",
        f"- `organic.json` currently points at: `{currently_wired or 'nothing (no organic.json for this cycle)'}`",
        f"- level at seam: {values['level_at_seam']:,.0f}; at Dec-15: {values['level_dec15']:,.0f}; at year end: {values['level_year_end']:,.0f}",
        *provenance.note_lines(repo),
        *([f"- **variant: `{provenance.variant}`** — this is not the point estimate. Wiring it replaces the point-estimate",
           "  paid level with this variant for the published mobile forecast; that is a deliberate decision, not a refresh."]
          if provenance.variant else []),
        "",
        "## To wire (not done here)",
        "",
        f"1. In `data-official/*/organic/organic.json` for seam {forecast_start}: set `paid_forecast.data_file` to",
        f"   `../marketing/{paths['parquet'].name}` and `paid_forecast.value_column` to `paid_dau_level_daily`.",
        "   There is no anchor: the level is read as delivered. Do not add `anchor_paid_dau`.",
        "2. Pin the new curve in `tests/test_organic.py` (Dec-15 level) and run `pytest tests/test_organic.py -q`.",
        "3. Update this directory's `_index.md` (which pull is live, numbers table) and the cycle `_index.md` ledger row for `p`.",
        "4. Mobile rerun; then re-measure `paid_seam_step`.",
        "5. Delete this file.",
        "",
    ]
    note.write_text("\n".join(lines))
    return note
