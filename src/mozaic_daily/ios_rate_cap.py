# -*- coding: utf-8 -*-
"""iOS-rate cap: a data correction for organic Fenix MAU training rows in countries where paid looks booked as organic.

Context (2027 organic mobile MAU draft, 2026-10-15). The paid/organic split counts a client as paid only when it arrived
through the Play Store with a gclid. In India and Indonesia organic Fenix MAU pulled 15 and 7 points above the same
country's Firefox iOS during the high spend period (April–October 2026) as the paid share of Fenix climbed past 30%,
while Brazil at the same paid share did not — consistent with campaign installs arriving by sideload or other stores and
being counted organic (``organic_mau/paid_organic_correlation.html`` section D). Brendan's decision: for the named
countries, from the named date, cap organic Fenix at the level it would reach by growing at the country's own
Firefox iOS rate; the positive excess is treated as paid and removed with no add-back. Documented for rollback in
``organic_mau/ios_rate_cap/IOS_RATE_CAP.md``.

Pure functions only. ``scripts/build_ios_rate_cap.py`` computes and writes the series; ``scripts/run_mobile_mau_forecast.py
--ios-rate-cap`` applies the written series to the organic training frame.
"""

from __future__ import annotations

from typing import Iterable

import numpy as np
import pandas as pd

CAP_COLUMNS = ["submission_date", "country", "organic_mau", "ios_mau", "ios_ratio_smoothed", "cap_level", "capped_mau", "removed_mau"]
DEFAULT_SMOOTHING_DAYS = 28


def ios_rate_cap_path(
    organic: pd.Series,
    ios: pd.Series,
    *,
    from_date: pd.Timestamp,
    smoothing_days: int = DEFAULT_SMOOTHING_DAYS,
) -> pd.DataFrame:
    """One country's cap path from ``from_date`` on.

    ``cap_level(t) = organic(from_date) * ios_ma(t) / ios_ma(from_date)`` where ``ios_ma`` is the trailing
    ``smoothing_days`` mean of the country's Firefox iOS MAU (so a one-day iOS wobble does not move the cap). The cap
    is exact at ``from_date`` (ratio 1) and ``removed_mau = max(organic - cap_level, 0)`` — the cap only ever removes.

    Both inputs are daily series indexed by date covering ``from_date`` (and the smoothing window before it, for the
    iOS mean; shorter histories fall back to the available days).
    """
    from_date = pd.Timestamp(from_date)
    organic = organic.astype(float).sort_index()
    ios = ios.astype(float).sort_index()
    if from_date not in organic.index or from_date not in ios.index:
        raise ValueError(f"from_date {from_date.date()} missing from the organic or iOS series (organic {organic.index.min().date()}.."
                         f"{organic.index.max().date()}, ios {ios.index.min().date()}..{ios.index.max().date()})")
    ios_ma = ios.rolling(smoothing_days, min_periods=1).mean()
    ratio = (ios_ma / ios_ma.loc[from_date]).loc[from_date:]
    organic = organic.loc[from_date:]
    ratio = ratio.reindex(organic.index)
    if ratio.isna().any():
        raise ValueError(f"iOS series does not cover every organic day from {from_date.date()}: {int(ratio.isna().sum())} missing")
    cap_level = organic.loc[from_date] * ratio
    capped = np.minimum(organic, cap_level)
    out = pd.DataFrame({
        "organic_mau": organic,
        "ios_mau": ios.reindex(organic.index),
        "ios_ratio_smoothed": ratio,
        "cap_level": cap_level,
        "capped_mau": np.round(capped),
        "removed_mau": organic - np.round(capped),
    })
    out.index.name = "submission_date"
    return out


def build_cap_frame(
    organic_by_country: pd.DataFrame,
    ios_by_country: pd.DataFrame,
    *,
    countries: Iterable[str],
    from_date: pd.Timestamp,
    smoothing_days: int = DEFAULT_SMOOTHING_DAYS,
) -> pd.DataFrame:
    """Long frame (``CAP_COLUMNS``) over the named countries; wide inputs are date x country."""
    parts = []
    for country in countries:
        if country not in organic_by_country.columns or country not in ios_by_country.columns:
            raise ValueError(f"{country}: no organic Fenix or Firefox iOS series to cap against")
        path = ios_rate_cap_path(organic_by_country[country], ios_by_country[country], from_date=from_date, smoothing_days=smoothing_days)
        parts.append(path.reset_index().assign(country=country))
    frame = pd.concat(parts, ignore_index=True)[CAP_COLUMNS]
    return frame


def apply_ios_rate_cap(
    training: pd.DataFrame,
    cap: pd.DataFrame,
    *,
    flag_column: str = "fenix_android",
    sentinel_attr: str = "ios_rate_cap_applied",
) -> tuple[pd.DataFrame, dict]:
    """Subtract ``removed_mau`` from the flagged training rows of the cap's (date, country) pairs.

    Refuses to run twice (sentinel), and refuses if the frame's ``y`` on those rows is not the ``organic_mau`` the cap was
    built from — the cap is a file computed once from a specific organic frame, so a different split or raw pull must
    rebuild it rather than silently subtract a stale excess. Returns a copy and a record for the sidecar.
    """
    if training.attrs.get(sentinel_attr):
        raise RuntimeError(f"apply_ios_rate_cap called twice (sentinel {sentinel_attr!r}); this would double-subtract")
    result = training.copy()
    result.attrs = dict(training.attrs)
    cap = cap.copy()
    cap["submission_date"] = pd.to_datetime(cap["submission_date"]).dt.normalize()
    keyed = cap.set_index(["submission_date", "country"])
    if keyed.index.duplicated().any():
        raise ValueError("cap frame has duplicate (submission_date, country) rows")

    mask = result[flag_column] == True  # noqa: E712
    keys = pd.MultiIndex.from_arrays([pd.to_datetime(result.loc[mask, "x"]).dt.normalize(), result.loc[mask, "country"]])
    hit = keys.isin(keyed.index)
    rows = result.index[mask][hit]
    if len(rows) != len(keyed):
        missing = keyed.index.difference(keys).tolist()[:3]
        raise ValueError(f"{len(keyed) - len(rows)} cap rows have no matching {flag_column} training row, e.g. {missing}")
    expected = keyed.loc[keys[hit], "organic_mau"].to_numpy()
    actual = result.loc[rows, "y"].astype(float).to_numpy()
    if not np.allclose(actual, expected, atol=0.5):
        worst = int(np.argmax(np.abs(actual - expected)))
        raise ValueError(f"training rows differ from the organic series the cap was built on (max |diff| {abs(actual - expected).max():,.0f} "
                         f"at {keys[hit][worst]}); rebuild the cap with scripts/build_ios_rate_cap.py on this frame")
    removed = keyed.loc[keys[hit], "removed_mau"].to_numpy()
    result.loc[rows, "y"] = np.round(actual - removed).astype("int64")
    result.attrs[sentinel_attr] = True

    by_country = cap.groupby("country")
    last_day = cap["submission_date"].max()
    record = {
        "countries": sorted(cap["country"].unique()),
        "from_date": str(cap["submission_date"].min().date()),
        "through": str(last_day.date()),
        "rows_capped": int(len(rows)),
        "removed_mau_last_day": {c: int(g.loc[g["submission_date"] == last_day, "removed_mau"].sum()) for c, g in by_country},
        "removed_mau_last_day_total": int(cap.loc[cap["submission_date"] == last_day, "removed_mau"].sum()),
        "peak_removed_share_of_organic": {c: float((g["removed_mau"] / g["organic_mau"]).max()) for c, g in by_country},
    }
    return result, record
