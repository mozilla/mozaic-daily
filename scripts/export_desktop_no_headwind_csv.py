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
Only `h`. The desktop curve still carries `l` (launch-on-login) and `o` (MozillaOnline), which are
per-tile bidirectional overlays baked into the parquet and not reversible at the display layer.

Precision
---------
The published columns are integers (already rounded), so each reconstructed value can differ from
the true unrounded pre-headwind curve by at most 1 DAU. Deriving from the published file instead of
recomputing from the parquet is deliberate: it guarantees the output is exactly the published
number minus the documented adjustment, which is the property a reader will check by hand.

Usage:
    python scripts/export_desktop_no_headwind_csv.py [--csv-dir DIR] [--dry-run]

Cycle-scoped: the constants below point at the August 2026 cycle. Repoint them at roll-forward.
"""

import argparse
import glob
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

# --- Cycle-scoped configuration (repoint at each roll-forward) -------------------------------
CSV_DIR = "data-official/2026-08/csv"
PUBLISHED_CURVES = "august_canonical_curves.csv"
PUBLISHED_SUMMARY = "august_dec15_summary.csv"

CURRENT_ADJUSTMENTS_DIR = "data-official/2026-08/adjustments"
PRIOR_ADJUSTMENTS_DIR = "data-official/2026-07/adjustments"

FORECAST_START = pd.Timestamp("2026-08-02")       # August desktop seam
PREV_FORECAST_START = pd.Timestamp("2026-07-06")  # July's seam
MEASUREMENT_DATE = pd.Timestamp("2026-12-15")
TROUGH_WINDOW_END = pd.Timestamp("2026-10-15")

# The one spec in an adjustments dir that IS the Win10 headwind. Any other spec that moves desktop
# is a hard error: this script must never quietly strip something else and call it the headwind.
WIN10_SPEC_FILENAME = "headwind.json"

# Loud, unmissable labels. The whole point of this artifact is that it can never be mistaken for
# the published canonical file, so the marker appears in the filename AND every value column.
LABEL = "NO_WIN10_HEADWIND"
FILE_MARKER = "DESKTOP_ONLY.WIN10_HEADWIND_REMOVED"


def render_desktop_linear_ramp(spec: dict, index: pd.DatetimeIndex) -> pd.Series:
    """Desktop leg of a `linear_ramp` spec, matching the canonical notebook's `render_adjustment`.

    Deliberately unclamped past `anchor_date`, because the published file it reverses is unclamped
    there too. See `data-official/2026-08/adjustments/_index.md` on the five diverging ramp
    implementations.
    """
    start = pd.Timestamp(spec["start_date"])
    anchor = pd.Timestamp(spec["anchor_date"])
    elapsed = np.maximum(0, (index - start).days)
    return pd.Series(spec.get("desktop_dau", 0) * elapsed / (anchor - start).days, index=index)


def load_desktop_headwind_ramp(
    adjustments_dir: str, index: pd.DatetimeIndex, forecast_start: pd.Timestamp
) -> pd.Series:
    """Signed desktop headwind series for one cycle, zeroed before that cycle's seam.

    Raises if the directory holds any spec that moves desktop other than the headwind, or any spec
    type whose reversal is not defined here.
    """
    spec_paths = sorted(glob.glob(f"{adjustments_dir}/*.json"))
    if not spec_paths:
        raise FileNotFoundError(
            f"No adjustment specs in {adjustments_dir}. Cannot know what to strip; an empty dir "
            f"would make this file identical to the published one while still claiming to differ."
        )

    total = pd.Series(0.0, index=index)
    win10_ramp = None
    for path in spec_paths:
        with open(path) as f:
            spec = json.load(f)
        if spec["type"] != "linear_ramp":
            raise ValueError(
                f"{path} has type={spec['type']!r}; this script only reverses 'linear_ramp'. "
                f"Add an explicit reversal before trusting the output."
            )
        leg = render_desktop_linear_ramp(spec, index)
        total += leg
        if os.path.basename(path) == WIN10_SPEC_FILENAME:
            win10_ramp = leg

    if win10_ramp is None:
        raise FileNotFoundError(f"{adjustments_dir} has no {WIN10_SPEC_FILENAME}; nothing to strip.")

    other_desktop = (total - win10_ramp).abs().max()
    if other_desktop != 0:
        raise ValueError(
            f"{adjustments_dir} holds a non-headwind spec that moves desktop by up to "
            f"{other_desktop:,.0f} DAU. Removing only {WIN10_SPEC_FILENAME} would leave this file "
            f"mislabelled. Extend this script to name each stripped component."
        )

    win10_ramp[index < forecast_start] = 0.0  # history is actuals; the ramp applies from the seam
    return win10_ramp


def strip_headwind(published: pd.Series, ramp: pd.Series) -> pd.Series:
    """Published (headwinded) column minus its ramp, re-rounded. NaNs stay NaN."""
    return (published - ramp).round(0)


def build_curves(published: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, pd.Series]]:
    """Desktop-only, headwind-free curve frame plus the ramps that were removed."""
    index = published.index
    ramps = {
        "august": load_desktop_headwind_ramp(CURRENT_ADJUSTMENTS_DIR, index, FORECAST_START),
        "july": load_desktop_headwind_ramp(PRIOR_ADJUSTMENTS_DIR, index, PREV_FORECAST_START),
    }
    frame = pd.DataFrame({"date": index.strftime("%Y-%m-%d")})
    # Actuals are telemetry and carry no adjustment, so they pass through untouched.
    frame["desktop_actuals"] = published["desktop_actuals"].values
    frame[f"desktop_prior_july_{LABEL}"] = strip_headwind(
        published["desktop_prior_july"], ramps["july"]
    ).values
    frame[f"desktop_current_august_{LABEL}"] = strip_headwind(
        published["desktop_current_august"], ramps["august"]
    ).values
    return frame, ramps


def build_summary(curves: pd.DataFrame, ramps: dict[str, pd.Series]) -> pd.DataFrame:
    """Dec-15 headline + summer trough, read back off the published-rounded curve columns."""
    indexed = curves.assign(date=pd.to_datetime(curves["date"])).set_index("date")
    current = indexed.loc[MEASUREMENT_DATE, f"desktop_current_august_{LABEL}"]
    prior = indexed.loc[MEASUREMENT_DATE, f"desktop_prior_july_{LABEL}"]
    window = indexed.loc[
        FORECAST_START:TROUGH_WINDOW_END, f"desktop_current_august_{LABEL}"
    ].dropna()
    return pd.DataFrame([{
        "series": "Desktop",
        "measurement_date": MEASUREMENT_DATE.strftime("%Y-%m-%d"),
        f"current_august_{LABEL}": int(current),
        f"prior_july_{LABEL}": int(prior),
        f"delta_vs_july_{LABEL}": int(current - prior),
        f"delta_pct_vs_july_{LABEL}": round((current / prior - 1) * 100, 3),
        f"summer_trough_min_{LABEL}": int(window.min()),
        f"summer_trough_date_{LABEL}": window.idxmin().strftime("%Y-%m-%d"),
        # Add these back to recover the published canonical figures exactly.
        "win10_headwind_added_back_august": int(-ramps["august"][MEASUREMENT_DATE]),
        "win10_headwind_added_back_july": int(-ramps["july"][MEASUREMENT_DATE]),
    }])


def verify(curves_path: Path, summary_path: Path, published: pd.DataFrame,
           published_summary: pd.DataFrame, ramps: dict[str, pd.Series]) -> None:
    """Re-read what was written and prove it re-derives the published canonical numbers."""
    written = pd.read_csv(curves_path, parse_dates=["date"]).set_index("date")
    written_summary = pd.read_csv(summary_path).set_index("series")

    assert list(written.columns) == [
        "desktop_actuals", f"desktop_prior_july_{LABEL}", f"desktop_current_august_{LABEL}"
    ], f"unexpected columns {list(written.columns)} -- mobile/ALL must not appear in this file"

    # 1. Actuals passed through unmodified.
    pd.testing.assert_series_equal(
        written["desktop_actuals"], published["desktop_actuals"], check_names=False
    )

    # 2. Round-trip: adding each ramp back reproduces the published column to <=1 DAU.
    for column, ramp_key, source in [
        (f"desktop_current_august_{LABEL}", "august", "desktop_current_august"),
        (f"desktop_prior_july_{LABEL}", "july", "desktop_prior_july"),
    ]:
        residual = (written[column] + ramps[ramp_key] - published[source]).abs().max()
        assert residual <= 1, (
            f"{column} + its ramp misses published {source} by {residual:,.0f} DAU -- the "
            f"reversal does not match the ramp that produced the published file."
        )

    # 3. Pre-seam rows are untouched by construction; assert it rather than trust it.
    pre_seam = written.index < PREV_FORECAST_START
    residual = (written.loc[pre_seam, f"desktop_prior_july_{LABEL}"]
                - published.loc[pre_seam, "desktop_prior_july"]).abs().max()
    assert residual == 0, f"prior-July column moved before July's seam by {residual:,.0f} DAU"
    assert written[f"desktop_current_august_{LABEL}"].first_valid_index() == FORECAST_START, (
        f"August column starts at {written[f'desktop_current_august_{LABEL}'].first_valid_index()}, "
        f"not the seam"
    )

    # 4. The published canonical Dec-15 figures re-derive from this file plus the ledger columns.
    row = written_summary.loc["Desktop"]
    expected = published_summary.set_index("series").loc["Desktop"]
    for label, reconstructed, published_value in [
        ("current_august", row[f"current_august_{LABEL}"] - row["win10_headwind_added_back_august"],
         expected["current_august"]),
        ("prior_july", row[f"prior_july_{LABEL}"] - row["win10_headwind_added_back_july"],
         expected["prior_july"]),
    ]:
        assert reconstructed == published_value, (
            f"{label}: subtracting the ledger column gives {reconstructed:,.0f}, but "
            f"{PUBLISHED_SUMMARY} publishes {published_value:,.0f}."
        )

    # 5. Summary re-derives from the curves file alone.
    curve_current = written.loc[MEASUREMENT_DATE, f"desktop_current_august_{LABEL}"]
    curve_prior = written.loc[MEASUREMENT_DATE, f"desktop_prior_july_{LABEL}"]
    assert row[f"current_august_{LABEL}"] == curve_current, "summary disagrees with the curves file"
    assert row[f"prior_july_{LABEL}"] == curve_prior, "summary disagrees with the curves file"
    assert row[f"delta_vs_july_{LABEL}"] == curve_current - curve_prior, (
        f"delta is {row[f'delta_vs_july_{LABEL}']:,.0f} but the two published columns differ by "
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
