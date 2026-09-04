#!/usr/bin/env python3
"""Export a DESKTOP-ONLY, WIN10-HEADWIND-REMOVED twin of the cycle's canonical CSVs.

Why this can be done arithmetically
-----------------------------------
The Win10 headwind (`h`) is a **display-layer** adjustment: the canonical notebook applies it to
the 28-day MA in `[compute-series]` via `load_adjustments` + `apply_net_adjustment`, and it is
never baked into the forecast parquets. So removing it is exactly

    no_headwind = published - headwind_ramp

with no model re-run and no access to the parquets. This script reproduces the notebook's ramp
semantics (`render_adjustment` for `type: "linear_ramp"`, unclamped past `anchor_date`, applied
only from each forecast's own seam forward) and subtracts it back out of the published columns.

The prior-July column is stripped the same way, using **July's own frozen spec**
(`data-official/2026-07/adjustments/headwind.json`, anchor -1,345,000, old 2026-04-01 ramp start)
applied from **July's** seam. July's artifacts are read-only here; nothing frozen is modified.

What is NOT removed
-------------------
Only `headwind.json`. The desktop curve still carries `l` (launch-on-login) and `o` (MozillaOnline),
which are per-tile bidirectional overlays baked into the parquet and not reversible at the display
layer, and any other display-layer spec in the cycle's `adjustments/` dir that moves desktop — those
are named on stdout when present.

Precision
---------
The published columns are integers (already rounded), so each reconstructed value can differ from
the true unrounded pre-headwind curve by at most 1 DAU. Deriving from the published file instead of
recomputing from the parquet is deliberate: it guarantees the output is exactly the published
number minus the documented adjustment, which is the property a reader will check by hand.

Usage:
    python scripts/export_desktop_no_headwind_csv.py [--csv-dir DIR] [--dry-run]

Cycle-scoped: the constants below point at the September 2026 cycle (repointed 2026-09-04). Repoint them at roll-forward.
NOTE: the published-column stems (CURRENT_COLUMN / PRIOR_COLUMN) must match the canonical notebook's CSV schema;
they are set to the August naming pattern shifted one cycle and must be checked against the first September export.
"""

import argparse
import glob
import json
import os
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from mozaic_daily.adjustments import render_adjustment  # noqa: E402

# --- Cycle-scoped configuration (repoint at each roll-forward) -------------------------------
CSV_DIR = "data-official/2026-09/csv"
PUBLISHED_CURVES = "september_canonical_curves.csv"
PUBLISHED_SUMMARY = "september_dec15_summary.csv"

CURRENT_ADJUSTMENTS_DIR = "data-official/2026-09/adjustments"
PRIOR_ADJUSTMENTS_DIR = "data-official/2026-08/adjustments"

FORECAST_START = pd.Timestamp("2026-09-02")       # September desktop seam
PREV_FORECAST_START = pd.Timestamp("2026-08-02")  # August's seam
MEASUREMENT_DATE = pd.Timestamp("2026-12-15")
TROUGH_WINDOW_END = pd.Timestamp("2026-10-15")

# The one spec in an adjustments dir that IS the Win10 headwind. Only this spec is stripped. Any
# other spec that moves desktop is left in place and named on stdout, so the output is exactly
# "published minus the Win10 headwind" whatever else the cycle's display layer carries.
WIN10_SPEC_FILENAME = "headwind.json"

# Published CSV column stems for the current and prior cycle (check against the notebook's export).
CURRENT_COLUMN = "desktop_current_september"
PRIOR_COLUMN = "desktop_prior_august"
CURRENT_KEY, PRIOR_KEY = "september", "august"

# Loud, unmissable labels. The whole point of this artifact is that it can never be mistaken for
# the published canonical file, so the marker appears in the filename AND every value column.
LABEL = "NO_WIN10_HEADWIND"
FILE_MARKER = "DESKTOP_ONLY.WIN10_HEADWIND_REMOVED"


def render_desktop_linear_ramp(spec: dict, index: pd.DatetimeIndex) -> pd.Series:
    """Desktop leg of a display-layer spec, rendered by the package's `render_adjustment`.

    The package renderer is the single source for the ramp math (unclamped past `anchor_date`),
    so this cannot drift from what the canonical notebook applies. Kept as a named helper because
    `export_desktop_ex_ir_cn_csv.py` reuses it.
    """
    return render_adjustment(spec, index)["desktop"]


def load_desktop_headwind_ramp(
    adjustments_dir: str, index: pd.DatetimeIndex, forecast_start: pd.Timestamp
) -> pd.Series:
    """Signed desktop Win10-headwind series for one cycle, zeroed before that cycle's seam.

    Only `headwind.json` is stripped. Every other spec in the directory is rendered too, and any
    that moves desktop is reported on stdout with its Dec-15 value so the reader knows the output
    still carries it. Raises on an empty directory or a missing headwind spec.
    """
    spec_paths = sorted(glob.glob(f"{adjustments_dir}/*.json"))
    if not spec_paths:
        raise FileNotFoundError(
            f"No adjustment specs in {adjustments_dir}. Cannot know what to strip; an empty dir "
            f"would make this file identical to the published one while still claiming to differ."
        )

    win10_ramp = None
    for path in spec_paths:
        with open(path) as f:
            spec = json.load(f)
        leg = render_adjustment(spec, index, spec_dir=os.path.dirname(path))["desktop"]
        if os.path.basename(path) == WIN10_SPEC_FILENAME:
            win10_ramp = leg
        elif leg.abs().max() != 0:
            at_measurement = leg.get(MEASUREMENT_DATE, float("nan"))
            print(
                f"NOTE: {path} also moves desktop ({at_measurement:+,.0f} DAU at "
                f"{MEASUREMENT_DATE.date()}); it is LEFT IN the {LABEL} output."
            )

    if win10_ramp is None:
        raise FileNotFoundError(f"{adjustments_dir} has no {WIN10_SPEC_FILENAME}; nothing to strip.")

    win10_ramp[index < forecast_start] = 0.0  # history is actuals; the ramp applies from the seam
    return win10_ramp


def strip_headwind(published: pd.Series, ramp: pd.Series) -> pd.Series:
    """Published (headwinded) column minus its ramp, re-rounded. NaNs stay NaN."""
    return (published - ramp).round(0)


def build_curves(published: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, pd.Series]]:
    """Desktop-only, headwind-free curve frame plus the ramps that were removed."""
    index = published.index
    ramps = {
        CURRENT_KEY: load_desktop_headwind_ramp(CURRENT_ADJUSTMENTS_DIR, index, FORECAST_START),
        PRIOR_KEY: load_desktop_headwind_ramp(PRIOR_ADJUSTMENTS_DIR, index, PREV_FORECAST_START),
    }
    frame = pd.DataFrame({"date": index.strftime("%Y-%m-%d")})
    # Actuals are telemetry and carry no adjustment, so they pass through untouched.
    frame["desktop_actuals"] = published["desktop_actuals"].values
    frame[f"{PRIOR_COLUMN}_{LABEL}"] = strip_headwind(published[PRIOR_COLUMN], ramps[PRIOR_KEY]).values
    frame[f"{CURRENT_COLUMN}_{LABEL}"] = strip_headwind(published[CURRENT_COLUMN], ramps[CURRENT_KEY]).values
    return frame, ramps


def build_summary(curves: pd.DataFrame, ramps: dict[str, pd.Series]) -> pd.DataFrame:
    """Dec-15 headline + summer trough, read back off the published-rounded curve columns."""
    indexed = curves.assign(date=pd.to_datetime(curves["date"])).set_index("date")
    current = indexed.loc[MEASUREMENT_DATE, f"{CURRENT_COLUMN}_{LABEL}"]
    prior = indexed.loc[MEASUREMENT_DATE, f"{PRIOR_COLUMN}_{LABEL}"]
    window = indexed.loc[
        FORECAST_START:TROUGH_WINDOW_END, f"{CURRENT_COLUMN}_{LABEL}"
    ].dropna()
    return pd.DataFrame([{
        "series": "Desktop",
        "measurement_date": MEASUREMENT_DATE.strftime("%Y-%m-%d"),
        f"current_{CURRENT_KEY}_{LABEL}": int(current),
        f"prior_{PRIOR_KEY}_{LABEL}": int(prior),
        f"delta_vs_{PRIOR_KEY}_{LABEL}": int(current - prior),
        f"delta_pct_vs_{PRIOR_KEY}_{LABEL}": round((current / prior - 1) * 100, 3),
        f"summer_trough_min_{LABEL}": int(window.min()),
        f"summer_trough_date_{LABEL}": window.idxmin().strftime("%Y-%m-%d"),
        # Add these back to recover the published canonical figures exactly.
        f"win10_headwind_added_back_{CURRENT_KEY}": int(-ramps[CURRENT_KEY][MEASUREMENT_DATE]),
        f"win10_headwind_added_back_{PRIOR_KEY}": int(-ramps[PRIOR_KEY][MEASUREMENT_DATE]),
    }])


def verify(curves_path: Path, summary_path: Path, published: pd.DataFrame,
           published_summary: pd.DataFrame, ramps: dict[str, pd.Series]) -> None:
    """Re-read what was written and prove it re-derives the published canonical numbers."""
    written = pd.read_csv(curves_path, parse_dates=["date"]).set_index("date")
    written_summary = pd.read_csv(summary_path).set_index("series")

    assert list(written.columns) == [
        "desktop_actuals", f"{PRIOR_COLUMN}_{LABEL}", f"{CURRENT_COLUMN}_{LABEL}"
    ], f"unexpected columns {list(written.columns)} -- mobile/ALL must not appear in this file"

    # 1. Actuals passed through unmodified.
    pd.testing.assert_series_equal(
        written["desktop_actuals"], published["desktop_actuals"], check_names=False
    )

    # 2. Round-trip: adding each ramp back reproduces the published column to <=1 DAU.
    for column, ramp_key, source in [
        (f"{CURRENT_COLUMN}_{LABEL}", CURRENT_KEY, CURRENT_COLUMN),
        (f"{PRIOR_COLUMN}_{LABEL}", PRIOR_KEY, PRIOR_COLUMN),
    ]:
        residual = (written[column] + ramps[ramp_key] - published[source]).abs().max()
        assert residual <= 1, (
            f"{column} + its ramp misses published {source} by {residual:,.0f} DAU -- the "
            f"reversal does not match the ramp that produced the published file."
        )

    # 3. Pre-seam rows are untouched by construction; assert it rather than trust it.
    pre_seam = written.index < PREV_FORECAST_START
    residual = (written.loc[pre_seam, f"{PRIOR_COLUMN}_{LABEL}"]
                - published.loc[pre_seam, PRIOR_COLUMN]).abs().max()
    assert residual == 0, f"prior-July column moved before July's seam by {residual:,.0f} DAU"
    assert written[f"{CURRENT_COLUMN}_{LABEL}"].first_valid_index() == FORECAST_START, (
        f"Current column starts at {written[f'{CURRENT_COLUMN}_{LABEL}'].first_valid_index()}, "
        f"not the seam"
    )

    # 4. The published canonical Dec-15 figures re-derive from this file plus the ledger columns.
    row = written_summary.loc["Desktop"]
    expected = published_summary.set_index("series").loc["Desktop"]
    for label, reconstructed, published_value in [
        (f"current_{CURRENT_KEY}", row[f"current_{CURRENT_KEY}_{LABEL}"] - row[f"win10_headwind_added_back_{CURRENT_KEY}"],
         expected[f"current_{CURRENT_KEY}"]),
        (f"prior_{PRIOR_KEY}", row[f"prior_{PRIOR_KEY}_{LABEL}"] - row[f"win10_headwind_added_back_{PRIOR_KEY}"],
         expected[f"prior_{PRIOR_KEY}"]),
    ]:
        assert reconstructed == published_value, (
            f"{label}: subtracting the ledger column gives {reconstructed:,.0f}, but "
            f"{PUBLISHED_SUMMARY} publishes {published_value:,.0f}."
        )

    # 5. Summary re-derives from the curves file alone.
    curve_current = written.loc[MEASUREMENT_DATE, f"{CURRENT_COLUMN}_{LABEL}"]
    curve_prior = written.loc[MEASUREMENT_DATE, f"{PRIOR_COLUMN}_{LABEL}"]
    assert row[f"current_{CURRENT_KEY}_{LABEL}"] == curve_current, "summary disagrees with the curves file"
    assert row[f"prior_{PRIOR_KEY}_{LABEL}"] == curve_prior, "summary disagrees with the curves file"
    assert row[f"delta_vs_{PRIOR_KEY}_{LABEL}"] == curve_current - curve_prior, (
        f"delta is {row[f'delta_vs_{PRIOR_KEY}_{LABEL}']:,.0f} but the two published columns differ by "
        f"{curve_current - curve_prior:,.0f} -- a reader subtracting them would get another answer."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv-dir", default=CSV_DIR,
                        help=f"directory holding the published canonical CSVs (default {CSV_DIR})")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the Dec-15 figures without writing files")
    args = parser.parse_args()

    csv_dir = Path(args.csv_dir)
    published = pd.read_csv(csv_dir / PUBLISHED_CURVES, parse_dates=["date"]).set_index("date")
    published_summary = pd.read_csv(csv_dir / PUBLISHED_SUMMARY)

    curves, ramps = build_curves(published)
    summary = build_summary(curves, ramps)

    if args.dry_run:
        print(summary.to_string(index=False))
        return

    curves_path = csv_dir / f"{Path(PUBLISHED_CURVES).stem}.{FILE_MARKER}.csv"
    summary_path = csv_dir / f"{Path(PUBLISHED_SUMMARY).stem}.{FILE_MARKER}.csv"
    curves.to_csv(curves_path, index=False)
    summary.to_csv(summary_path, index=False)
    print(f"wrote {curves_path}  ({len(curves)} rows x {len(curves.columns)} cols)")
    print(f"wrote {summary_path}")

    verify(curves_path, summary_path, published, published_summary, ramps)
    print("\nVerified:")
    print("  actuals column byte-identical to the published file")
    print("  adding each cycle's ramp back reproduces the published columns (<=1 DAU)")
    print("  published Dec-15 figures re-derive via the win10_headwind_added_back_* columns")
    print("  no mobile or ALL columns present")
    print()
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
