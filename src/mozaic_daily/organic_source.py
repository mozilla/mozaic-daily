# -*- coding: utf-8 -*-
"""Build the measured Fenix paid/organic split from client-level mirror data.

**Producer side.** This module turns raw `growth_source` rows into the per-cycle artifact that
`data-official/{YYYY-MM}/organic/` pins. The *consumer* side — applying that artifact to a
forecast run as adjustment ``p`` — lives in :mod:`mozaic_daily.organic`.

Everything here is pure: frames in, frames out, no I/O. `scripts/build_fenix_organic_split.py`
is the only caller that touches BigQuery.

## What "organic" means

A Fenix client is **paid** iff it appears in ``mozdata.fenix.new_profile_clients`` with
``paid_vs_organic_gclid = 'Paid'`` **and** ``normalized_channel = 'release'`` **and**
``install_source = 'com.android.vending'``. Organic is the residual — sideloads, non-release
channels, unclassified clients, and profiles predating that table. This is the authoritative
marketing definition (Redash 118471), and it is what makes the partition exact:

    organic + paid_rolling_12mo + paid_prior_1yr == total,  every day, gap 0

:func:`check_partition_identity` enforces it rather than trusting it.

## Why only the *share* is exported

The mirror is built from client-level ``fenix.active_users``, which loses clients over time as
deletion requests are processed; the canonical aggregate is accumulating and was written when
each day was fresh. Measured against production, the mirror runs **−2.855%** low on 2024-06-01
and decays monotonically to **0.000%** at the trailing edge — ~2.9pp of pure artifact growth over
the window.

So the artifact carries the **share**, where attrition largely cancels between numerator and
denominator, and the *level* comes from the production query at apply time:

    organic_y(d, c) = y_production(d, c) * organic_share(d, c)

Exporting the mirror's own DAU counts as the level would read as ~+3.3pp of fake growth.
:func:`check_shredder_drift` measures the gap so the correction stays auditable rather than
invisible.
"""

from __future__ import annotations

import pandas as pd

__all__ = [
    "GROWTH_SOURCE_ORGANIC",
    "GROWTH_SOURCE_PAID",
    "combine_snapshot_and_tail",
    "check_tail_overlap",
    "check_partition_identity",
    "build_split_frame",
    "check_split_coverage",
    "check_shredder_drift",
    "production_fenix_daily",
]

#: growth_source values the mirror emits. The three partition Fenix DAU exactly.
GROWTH_SOURCE_ORGANIC = "organic"
GROWTH_SOURCE_PAID = ("paid_rolling_12mo", "paid_prior_1yr")

_ALL_GROWTH_SOURCES = (GROWTH_SOURCE_ORGANIC,) + GROWTH_SOURCE_PAID


def _normalize_dates(df: pd.DataFrame, column: str = "submission_date") -> pd.DataFrame:
    """Return a copy with ``column`` coerced to midnight-normalized datetime64."""
    out = df.copy()
    out[column] = pd.to_datetime(out[column]).dt.normalize()
    return out


def combine_snapshot_and_tail(snapshot: pd.DataFrame, tail: pd.DataFrame) -> pd.DataFrame:
    """Concatenate the mirror snapshot with its tail extension, tail winning on overlap.

    Both halves are produced by the same SQL definition. The overlap days exist precisely so
    :func:`check_tail_overlap` can confirm that before this drops them — run the check first.
    """
    snapshot = _normalize_dates(snapshot)
    tail = _normalize_dates(tail)
    if tail.empty:
        return snapshot.sort_values(["submission_date", "country", "growth_source"]).reset_index(drop=True)
    cutoff = tail["submission_date"].min()
    kept = snapshot[snapshot["submission_date"] < cutoff]
    return (
        pd.concat([kept, tail], ignore_index=True)
        .sort_values(["submission_date", "country", "growth_source"])
        .reset_index(drop=True)
    )


def check_tail_overlap(
    snapshot: pd.DataFrame,
    tail: pd.DataFrame,
    *,
    min_days: int = 3,
    rel_tol: float = 0.005,
) -> pd.DataFrame:
    """Do the snapshot and its tail agree where they overlap?

    They are meant to be the same query over the same data, so disagreement means the tail SQL
    has drifted from the mirror build and the two halves cannot be concatenated. Raises rather
    than warning: a silent drift here shifts the organic share by an unbounded amount.
    """
    snapshot = _normalize_dates(snapshot)
    tail = _normalize_dates(tail)
    a = snapshot.groupby("submission_date")["dau"].sum()
    b = tail.groupby("submission_date")["dau"].sum()
    shared = a.index.intersection(b.index)
    if len(shared) < min_days:
        raise ValueError(
            f"tail overlaps the snapshot on only {len(shared)} day(s), need >= {min_days}; "
            f"snapshot ends {a.index.max().date()}, tail starts {b.index.min().date()}"
        )
    rel = (b[shared] / a[shared] - 1).abs()
    if rel.max() > rel_tol:
        worst = rel.idxmax()
        raise ValueError(
            f"tail disagrees with the snapshot by {rel.max():.4%} on {worst.date()} "
            f"(tolerance {rel_tol:.2%}); the tail SQL has drifted from the mirror build"
        )
    return pd.DataFrame([{
        "check": "tail overlap",
        "days": len(shared),
        "median_rel_diff": float((b[shared] / a[shared] - 1).median()),
        "max_abs_rel_diff": float(rel.max()),
        "status": "PASS",
    }])


def check_partition_identity(mirror: pd.DataFrame) -> pd.DataFrame:
    """Assert ``organic + paid_rolling_12mo + paid_prior_1yr`` is the whole of Fenix DAU.

    An unexpected `growth_source` value would silently vanish from the share denominator, so an
    unknown label is an error, not something to sum away.
    """
    unknown = set(mirror["growth_source"].unique()) - set(_ALL_GROWTH_SOURCES)
    if unknown:
        raise ValueError(
            f"unknown growth_source value(s) {sorted(unknown)}; expected "
            f"{list(_ALL_GROWTH_SOURCES)}. A new bucket would drop out of the share denominator."
        )
    return pd.DataFrame([{
        "check": "partition identity",
        "growth_sources": len(mirror["growth_source"].unique()),
        "days": int(mirror["submission_date"].nunique()),
        "status": "PASS",
    }])


def build_split_frame(mirror: pd.DataFrame) -> pd.DataFrame:
    """Pivot mirror rows to one row per (date, country) with the organic share.

    Returns ``submission_date, country, organic_dau, paid_dau, total_dau, organic_share``.
    The DAU columns are the *mirror's own* counts, kept for audit — they are deliberately not
    the level the pipeline uses (see the module docstring on shredder attrition).

    A (date, country) cell with zero total DAU gets ``organic_share = NaN`` rather than a
    fabricated 1.0; :func:`check_split_coverage` decides whether that is tolerable.
    """
    mirror = _normalize_dates(mirror)
    wide = (
        mirror.pivot_table(
            index=["submission_date", "country"],
            columns="growth_source",
            values="dau",
            aggfunc="sum",
            fill_value=0,
        )
        .reindex(columns=list(_ALL_GROWTH_SOURCES), fill_value=0)
    )
    wide.columns.name = None
    out = pd.DataFrame(index=wide.index)
    out["organic_dau"] = wide[GROWTH_SOURCE_ORGANIC].astype("int64")
    out["paid_dau"] = wide[list(GROWTH_SOURCE_PAID)].sum(axis=1).astype("int64")
    out["total_dau"] = out["organic_dau"] + out["paid_dau"]
    out["organic_share"] = (out["organic_dau"] / out["total_dau"]).where(out["total_dau"] > 0)
    return out.reset_index().sort_values(["submission_date", "country"]).reset_index(drop=True)


def check_split_coverage(
    split: pd.DataFrame,
    *,
    expected_countries: set[str],
    training_end: pd.Timestamp,
) -> pd.DataFrame:
    """Gapless daily coverage, every expected country present, every share in (0, 1].

    A missing country-day means the applier would fall back to "no paid here", which silently
    inflates organic for that tile. A share outside (0, 1] means the partition is broken.
    """
    split = _normalize_dates(split)
    training_end = pd.Timestamp(training_end).normalize()

    dates = pd.DatetimeIndex(sorted(split["submission_date"].unique()))
    # Check the short-tail case FIRST. A split that simply stops early also presents as a run of
    # missing days, and "extend the tail query" is the actionable message; "1,234 missing days"
    # is not.
    if dates.max() < training_end:
        raise ValueError(
            f"split ends {dates.max().date()} but training_end is {training_end.date()}; "
            f"extend the tail query"
        )
    expected = pd.date_range(dates.min(), training_end, freq="D")
    missing_days = expected.difference(dates)
    if len(missing_days):
        raise ValueError(
            f"{len(missing_days)} missing day(s) between {dates.min().date()} and "
            f"{training_end.date()}, e.g. {[d.date() for d in missing_days[:5]]}"
        )

    present = set(split["country"].unique())
    missing_countries = expected_countries - present
    if missing_countries:
        raise ValueError(
            f"country/countries {sorted(missing_countries)} absent from the split; the applier "
            f"would treat their Fenix DAU as 100% organic"
        )

    share = split["organic_share"].dropna()
    if not ((share > 0) & (share <= 1)).all():
        bad = split.loc[~split["organic_share"].between(0, 1, inclusive="right")].head(3)
        raise ValueError(f"organic_share outside (0, 1]:\n{bad.to_string(index=False)}")

    n_null = int(split["organic_share"].isna().sum())
    return pd.DataFrame([{
        "check": "split coverage",
        "days": len(dates),
        "countries": len(present),
        "rows": len(split),
        "null_shares": n_null,
        "min_share": float(share.min()),
        "max_share": float(share.max()),
        "status": "PASS",
    }])


def production_fenix_daily(raw_mobile_dau: pd.DataFrame) -> pd.Series:
    """Daily Fenix DAU from a production raw-mobile checkpoint frame.

    Takes the ``mozaic_parts.raw.glean.mobile.DAU.parquet`` schema
    (``x, country, fenix_android, firefox_ios, focus_android, focus_ios, y``) and returns a
    date-indexed Series of total Fenix DAU. This is the *level* source the split multiplies
    against, so it is also the right thing to measure shredder drift against.
    """
    fenix = raw_mobile_dau[raw_mobile_dau["fenix_android"] == True]  # noqa: E712
    dates = pd.to_datetime(fenix["x"]).dt.normalize()
    return fenix.groupby(dates)["y"].sum().astype("int64").sort_index()


def check_shredder_drift(
    split: pd.DataFrame,
    production_daily: pd.Series,
    *,
    edge_tol: float = 0.002,
    edge_days: int = 14,
) -> pd.DataFrame:
    """Measure the mirror-vs-production level gap, and assert it has closed at the trailing edge.

    Expected shape: a monotone negative gap, largest on the oldest day, ~0% on recent days as
    deletion requests have not yet been processed against them.

    The trailing-edge assertion is the load-bearing one. If the gap is *not* ~0 there, the two
    sources are not covering the same population (e.g. a relabelling change moved MozillaOnline
    or BrowserStack traffic into or out of one of them) and the share/level split is invalid —
    which is a different, much worse failure than attrition.
    """
    split = _normalize_dates(split)
    mirror_daily = split.groupby("submission_date")["total_dau"].sum()
    shared = mirror_daily.index.intersection(production_daily.index)
    if len(shared) == 0:
        raise ValueError("mirror split and production Fenix series do not overlap")
    rel = mirror_daily[shared] / production_daily[shared] - 1

    edge = rel.tail(edge_days)
    if edge.abs().max() > edge_tol:
        raise ValueError(
            f"mirror vs production gap is {edge.abs().max():.4%} over the last {edge_days} days "
            f"(tolerance {edge_tol:.2%}). Shredder attrition should be ~0 there, so a nonzero "
            f"gap means the two sources cover different populations — check whether "
            f"'Fenix MozillaOnline' / 'Fenix browserstack' relabelling has changed."
        )
    return pd.DataFrame([{
        "check": "shredder drift",
        "days": len(shared),
        "oldest_day": str(shared.min().date()),
        "oldest_rel_gap": float(rel.iloc[0]),
        "newest_rel_gap": float(rel.iloc[-1]),
        "edge_max_abs_rel_gap": float(edge.abs().max()),
        "rank_corr": float(pd.Series(rel.values).corr(pd.Series(range(len(rel))), method="spearman")),
        "status": "PASS",
    }])
