#!/usr/bin/env python3
"""Fold a cycle's canonical curve into the KPI workbook's "Official Forecast Data" tab.

Reads the tab's current CSV export (downloaded from the Google Sheet) plus the cycle's
canonical curves CSV, and writes the full replacement table under
``data-official/{cycle}/kpi_sheet/`` together with a verbatim copy of the export it read, a
meta JSON, and a render-check plot. Never uploads; never overwrites. Logic in
``mozaic_daily.kpi_sheet`` / ``kpi_sheet_checks``; driven by the ``/update-kpi-sheet`` skill.

    python scripts/build_kpi_sheet_update.py --sheet-export ~/Downloads/'…Official Forecast Data(2).csv' \
        --cycle 2026-09 --publish-date 2026-09-17 --rename AUG=JUL \
        --expect-dec15 desktop=49332443 --expect-dec15 mobile=18214594 [--dry-run] [--draft]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mozaic_daily.kpi_sheet import (  # noqa: E402
    CURRENT, FUTURE, PRODUCTS, UpdatePlan, assemble_update, block_inventory, format_for_sheet,
    month_label, outgoing_created_on, outgoing_forecast_start, read_sheet_export,
)
from mozaic_daily.kpi_sheet_checks import check_update, describe_update  # noqa: E402

REPO = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--sheet-export", required=True, type=Path, help="the tab's CSV export from Google Sheets")
    p.add_argument("--cycle", required=True, help="YYYY-MM of the cycle being published")
    p.add_argument("--curves", type=Path, help="canonical curves CSV (default: the cycle's csv/<month>_canonical_curves.csv)")
    p.add_argument("--publish-date", default=date.today().isoformat(), help="created_on/updated_on of the new rows (default today)")
    p.add_argument("--draft", action="store_true", help="append as FUTURE * instead of promoting to CURRENT")
    p.add_argument("--demote-to", help="label the outgoing CURRENT takes (default: month of its created_on)")
    p.add_argument("--rename", action="append", default=[], metavar="OLD=NEW", help="extra label fix, repeatable")
    p.add_argument("--expect-dec15", action="append", default=[], metavar="PRODUCT=INT",
                   help="lock the new line's Dec-15 value; required per product unless --no-dec15-lock")
    p.add_argument("--no-dec15-lock", action="store_true")
    p.add_argument("--out-dir", type=Path, help="default data-official/<cycle>/kpi_sheet")
    p.add_argument("--plots-dir", type=Path, help="default data-official/<cycle>/plots")
    p.add_argument("--dry-run", action="store_true", help="print the inventory and plan, write nothing")
    return p.parse_args()


def parse_pairs(items: list[str], what: str) -> dict[str, str]:
    pairs = {}
    for item in items:
        key, sep, value = item.partition("=")
        if not sep or not key or not value:
            raise SystemExit(f"--{what} expects KEY=VALUE, got {item!r}")
        pairs[key.strip()] = value.strip()
    return pairs


def month_name(cycle: str) -> str:
    return pd.Timestamp(f"{cycle}-01").strftime("%B").lower()


def sha1(path: Path) -> str:
    return hashlib.sha1(path.read_bytes()).hexdigest()


def render_check_plot(out: pd.DataFrame, plan: UpdatePlan, info: dict, path: Path) -> None:
    """Both products: the new prior line (with its blanks) and the new forecast line, the outgoing
    forecast dotted behind it. Y ticks carry two decimals of millions so narrow ranges stay legible."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    demoted = plan.label_mapping().get(CURRENT, CURRENT)
    fig, axes = plt.subplots(len(PRODUCTS), 1, figsize=(12, 4.2 * len(PRODUCTS)), sharex=True)
    for ax, product in zip(axes, PRODUCTS):
        sub = out[out["product"] == product]
        for name, style in ((f"{plan.install_as} prior forecasts", dict(color="#9b8bd6", lw=2.2)),
                            (f"{plan.install_as} forecast", dict(color="#1f5fbf", lw=2.2)),
                            (f"{demoted} forecast", dict(color="#888888", lw=1.2, ls=":"))):
            line = sub[sub["forecast_name"] == name].set_index("submission_date")["dau_28_ma"]
            if len(line):
                ax.plot(line.index, line.values, label=name, **style)
        ax.axvline(pd.Timestamp(info["handoff_gap_date"]), color="#cc4444", lw=0.8, ls="--")
        ax.axvline(pd.Timestamp(info["seam"]), color="#222222", lw=0.8, ls="--")
        ax.set_title(f"{product}: {plan.install_as} lines as written · Dec-15 {info['products'][product]['dec15']:,}"
                     f" · seam step {info['products'][product]['seam_step']:+,}"
                     f" · handoff-gap step {info['products'][product]['handoff_gap_step']:+,}", fontsize=10)
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1e6:.2f}M"))
        ax.grid(alpha=0.3)
        ax.legend(loc="lower left", fontsize=8)
    axes[-1].xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%b %d"))
    fig.suptitle(f"KPI sheet update · publish {info['publish_date']} · dashed: handoff gap {info['handoff_gap_date']}"
                 f" and seam {info['seam']}", fontsize=11)
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=130)
    plt.close(fig)


def main() -> None:
    args = parse_args()
    month = month_name(args.cycle)
    cycle_dir = REPO / "data-official" / args.cycle
    curves_path = args.curves or cycle_dir / "csv" / f"{month}_canonical_curves.csv"
    out_dir = args.out_dir or cycle_dir / "kpi_sheet"
    plots_dir = args.plots_dir or cycle_dir / "plots"

    sheet = read_sheet_export(args.sheet_export)
    curves = pd.read_csv(curves_path, parse_dates=["date"])
    curve_columns = {product: f"{product}_current_{month}" for product in PRODUCTS}
    seam = min(curves.loc[curves[c].notna(), "date"].min() for c in curve_columns.values())

    expected = {k: int(v) for k, v in parse_pairs(args.expect_dec15, "expect-dec15").items()}
    if not args.no_dec15_lock and set(expected) != set(PRODUCTS):
        raise SystemExit(f"--expect-dec15 needed for {sorted(PRODUCTS)} (got {sorted(expected)}), or pass --no-dec15-lock")
    demote_to = None if args.draft else (args.demote_to or month_label(outgoing_created_on(sheet)))
    plan = UpdatePlan(seam=seam, publish_date=pd.Timestamp(args.publish_date), curve_columns=curve_columns,
                      install_as=FUTURE if args.draft else CURRENT, demote_to=demote_to,
                      renames=parse_pairs(args.rename, "rename"), expected_dec15=expected)

    print("=== Sheet as exported")
    print(block_inventory(sheet).to_string(index=False))
    print(f"\n=== Plan\n  curves: {curves_path}\n  seam (first day of {curve_columns['desktop']}): {seam.date()}"
          f"\n  outgoing CURRENT seam: {outgoing_forecast_start(sheet).date()} (created {outgoing_created_on(sheet).date()})"
          f"\n  install as: {plan.install_as}   label renames: {plan.label_mapping()}"
          f"\n  publish date: {plan.publish_date.date()}   Dec-15 lock: {expected or 'none'}")
    if args.dry_run:
        print("\n--dry-run: nothing written")
        return

    stem = f"official_forecast_data.{plan.publish_date.date()}"
    out_csv = out_dir / f"{stem}.csv"
    if out_csv.exists():
        raise SystemExit(f"refusing to overwrite {out_csv}; pick another --publish-date or move it aside")

    out = assemble_update(sheet, curves, plan)
    check_update(out, sheet, curves, plan)
    info = describe_update(out, sheet, plan)

    out_dir.mkdir(parents=True, exist_ok=True)
    source_copy = out_dir / "source_data" / f"sheet_export.{plan.publish_date.date()}.csv"
    source_copy.parent.mkdir(exist_ok=True)
    shutil.copyfile(args.sheet_export, source_copy)
    format_for_sheet(out).to_csv(out_csv, index=False)
    plot_path = plots_dir / f"kpi_sheet_{plan.publish_date.date()}.png"
    render_check_plot(out, plan, info, plot_path)
    meta = {**info, "sheet_export": str(args.sheet_export), "sheet_export_sha1": sha1(args.sheet_export),
            "curves": str(curves_path.relative_to(REPO)), "curves_sha1": sha1(curves_path),
            "curve_columns": curve_columns, "output_csv": str(out_csv.relative_to(REPO)),
            "output_sha1": sha1(out_csv), "check_plot": str(plot_path.relative_to(REPO))}
    (out_dir / f"{stem}.meta.json").write_text(json.dumps(meta, indent=2) + "\n")

    print(f"\n=== Written\n  {out_csv}  ({len(out):,} rows; {len(sheet):,} in)\n  {source_copy}\n  {plot_path}\n  {out_dir / f'{stem}.meta.json'}")
    for product, numbers in info["products"].items():
        print(f"  {product}: Dec-15 {numbers['dec15']:,} (outgoing {numbers['outgoing_dec15']:,}, "
              f"{numbers['dec15'] - numbers['outgoing_dec15']:+,}); seam step {numbers['seam_step']:+,}; "
              f"handoff-gap step at {info['handoff_gap_date']} {numbers['handoff_gap_step']:+,}")
    print("\nNext: upload the CSV to the sheet by hand (replace the whole tab); nothing here touches the sheet or BigQuery.")


if __name__ == "__main__":
    main()
