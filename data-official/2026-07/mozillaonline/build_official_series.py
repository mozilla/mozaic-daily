"""Build the official MozillaOnline migration daily series for the `o` overlay.

Reads Brad Ochocki Szasz's official model export
(`source_data/mozilla_online_forecast_jul.csv`, columns
`submission_date,type,dau,dau_28ma`) and produces the overlay artifact that the
bidirectional `o` applier consumes:

    mozillaonline_migration_model.official.2026-06-29.parquet
        index  : target_date (DatetimeIndex, normalized), 2026-01-01 .. 2027-12-31
        columns: migration_dau_daily  (the daily value the applier uses)
                 migration_dau_ma      (28d-MA; display only)

Horizon handling:
  - Before the official series starts (pre 2026-06-01): 0 (no migration).
  - Official daily values 2026-06-01 .. 2026-12-31 copied verbatim from `dau`.
  - After 2026-12-31 (through the 2027-12-31 forecast horizon): HELD FLAT at the
    2026-12-31 28d-MA level (~550K). Per the July cycle decision, the migration
    contribution is frozen beyond the modeled horizon rather than allowed to
    cliff to zero. Holding the *daily* flat at the final 28d-MA makes the 28d-MA
    contribution flat at exactly that level.

This is a reproducible producer (not throwaway). Re-run after dropping a new
official CSV; the applier contract is unchanged.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
SOURCE_CSV = HERE / "source_data" / "mozilla_online_forecast_jul.csv"
OUT_PARQUET = HERE / "mozillaonline_migration_model.official.2026-06-29.parquet"

# Full forecast horizon (mozaic forecast_end = Dec 31 of next year).
HORIZON_START = pd.Timestamp("2026-01-01")
HORIZON_END = pd.Timestamp("2027-12-31")
HOLD_FLAT_FROM = pd.Timestamp("2027-01-01")


def build_series() -> tuple[pd.DataFrame, pd.Timestamp, float]:
    """Return (series, last_official_date, hold_value)."""
    raw = pd.read_csv(SOURCE_CSV, parse_dates=["submission_date"])
    raw = raw.set_index("submission_date").sort_index()

    full_index = pd.date_range(HORIZON_START, HORIZON_END, freq="D")
    daily = pd.Series(0.0, index=full_index, name="migration_dau_daily")
    ma = pd.Series(0.0, index=full_index, name="migration_dau_ma")

    # Copy the official values over their covered range.
    covered = raw.index.intersection(full_index)
    daily.loc[covered] = raw.loc[covered, "dau"].astype("float64")
    ma.loc[covered] = raw.loc[covered, "dau_28ma"].astype("float64")

    # Hold flat beyond the official horizon at the final 28d-MA level (~550K).
    last_official_date = raw.index.max()  # 2026-12-31
    hold_value = float(raw.loc[last_official_date, "dau_28ma"])
    tail = full_index >= HOLD_FLAT_FROM
    daily.loc[tail] = hold_value
    ma.loc[tail] = hold_value

    out = pd.concat([daily, ma], axis=1)
    out.index.name = "target_date"
    return out, last_official_date, hold_value


def main() -> None:
    out, last_official_date, hold_value = build_series()
    out.to_parquet(OUT_PARQUET)

    def at(date: str) -> tuple[float, float]:
        ts = pd.Timestamp(date)
        return float(out.loc[ts, "migration_dau_daily"]), float(out.loc[ts, "migration_dau_ma"])

    print(f"Wrote {OUT_PARQUET.name}")
    print(f"  rows={len(out)}  coverage={out.index.min().date()} .. {out.index.max().date()}")
    print(f"  last official date={last_official_date.date()}  hold-flat value={hold_value:,.0f}")
    for d in ["2026-06-02", "2026-07-06", "2026-12-15", "2026-12-31", "2027-06-15", "2027-12-31"]:
        daily_v, ma_v = at(d)
        print(f"  {d}: daily={daily_v:>12,.0f}   ma28={ma_v:>12,.0f}")


if __name__ == "__main__":
    main()
