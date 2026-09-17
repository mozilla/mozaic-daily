#!/usr/bin/env python3
"""Compare two cycles' adjustment effects: how each code's Dec-15 impact moved, and why.

Reads ``data-official/<cycle>/adjustment_combinatorics/adjustment_effects.csv`` for the prior and
the current cycle (written by ``scripts/export_adjustment_effects.py``) and prints, per platform and
code, the realized Dec-15 effect on each side, the delta, and the exact split of that delta into

* ``from_curve_change``        -- the delivered curve / spec value moved (prior pass-through held), and
* ``from_pass_through_change`` -- the model absorbed a different fraction of it (current curve held).

Display-layer codes are exact, so their whole delta is a curve change. Codes present on one side
only are ``added`` / ``removed``. The default view is Shapley (order-free); ``--view single`` or
``--view marginal`` compare the other two.

Run (from the repo root):

    source .venv/bin/activate && python scripts/compare_adjustment_effects.py --prior 2026-08 --current 2026-09

Writes ``adjustment_effects_vs_<prior>.csv`` beside the current cycle's effects file unless
``--no-write``. Never runs the model. Logic in ``mozaic_daily.adjustment_effects.compare_effects``.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from mozaic_daily.adjustment_effects import EFFECT_VIEWS, compare_effects  # noqa: E402

EFFECTS_NAME = "adjustment_effects.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--prior", required=True, help="prior cycle, e.g. 2026-08")
    parser.add_argument("--current", required=True, help="current cycle, e.g. 2026-09")
    parser.add_argument("--view", default="shapley", choices=EFFECT_VIEWS)
    parser.add_argument("--no-write", action="store_true", help="print only")
    return parser.parse_args()


def effects_path(cycle: str) -> Path:
    path = REPO / "data-official" / cycle / "adjustment_combinatorics" / EFFECTS_NAME
    if not path.exists():
        raise SystemExit(f"no {EFFECTS_NAME} for {cycle}; run scripts/export_adjustment_effects.py --cycle {cycle}")
    return path


def print_table(table: pd.DataFrame, prior: str, current: str) -> None:
    print(f"{'platform':<8s} {'code':<5s} {'status':<8s} {prior:>14s} {current:>14s} {'delta':>12s} "
          f"{'curve moved':>12s} {'pass-thru':>12s}   nominal {prior} -> {current}")
    for _, row in table.iterrows():
        print(f"{row['platform']:<8s} {row['code']:<5s} {row['status']:<8s} "
              f"{_fmt(row['prior_effect']):>14s} {_fmt(row['current_effect']):>14s} {_fmt(row['delta_effect'], sign=True):>12s} "
              f"{_fmt(row['from_curve_change'], sign=True):>12s} {_fmt(row['from_pass_through_change'], sign=True):>12s}   "
              f"{_fmt(row['prior_nominal'])} -> {_fmt(row['current_nominal'])}")


def _fmt(value, sign: bool = False) -> str:
    if pd.isna(value):
        return "-"
    return f"{value:+,.0f}" if sign else f"{value:,.0f}"


def main() -> None:
    args = parse_args()
    prior = pd.read_csv(effects_path(args.prior))
    current = pd.read_csv(effects_path(args.current))
    table = compare_effects(prior, current, view=args.view)
    print(f"Adjustment effects, {args.prior} -> {args.current}, Dec-15 28d-MA, view = {args.view}\n")
    print_table(table, args.prior, args.current)
    if not args.no_write:
        out = effects_path(args.current).with_name(f"adjustment_effects_vs_{args.prior}.csv")
        table.to_csv(out, index=False)
        print(f"\nwrote {out.relative_to(REPO)}")


if __name__ == "__main__":
    main()
