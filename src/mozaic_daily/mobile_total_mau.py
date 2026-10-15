# -*- coding: utf-8 -*-
"""Compose TOTAL mobile MAU = our ORGANIC forecast + the marketing team's PAID plan (2027 mobile MAU draft, 2026-10-15).

Pure functions; ``scripts/compose_mobile_total_mau.py`` does the I/O. The rules, each the same as production's `p` where one exists:

* **History rows are the real actuals.** Total = the raw pull on every training day; the paid shown for history is simply
  raw − organic (= the measured paid the split removed + the India/Indonesia cap excess + the held-flat share before the split's
  first day). Nothing is estimated for history.
* **Paid is a level, added after the model.** Forecast rows get the delivered paid plan (bridge + 2027). No Prophet interaction.
* **Only Fenix and the world move.** Firefox iOS and the Focus apps carry no paid acquisition.
* **The India/Indonesia cap excess** ("inauthentic organic": organic removed from training because it outgrew the country's own iOS
  as paid share climbed) is its own column, ``organic_inauthentic_<pop>``: the removed series over history, then HELD FLAT at its
  last value through 2027 (Brendan, 2026-10-15: "keep it flat, but document that choice"). It is neither in our organic model nor
  in marketing's plan (which counts Play-Store-attributed installs only); flat is the no-information choice, as for the desktop
  Linux bot component. ``policy='drop'`` zeroes the forecast part (opens a ~0.9% hole at the seam in India). Total =
  authentic organic + inauthentic organic + paid.
* **Per country**: forecast paid is allocated by a fixed share vector (each country's share of measured paid over the last 28
  days); the cap excess goes to its own country.
* **Bands**: organic and paid bands are each quoted as delivered; the total band treats them as independent — half-widths
  added in quadrature around the total point — and says so. Quantiles never add directly.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

PAID_POPULATIONS = ("world", "fenix_android")      # the only populations paid lands in


def cap_excess_series(cap: pd.DataFrame, training_end: pd.Timestamp, horizon_end: pd.Timestamp, policy: str = "hold_flat") -> pd.DataFrame:
    """Date x country frame of the cap's removed MAU ("inauthentic organic"): as removed over history (zero before the cap starts),
    then the last value held flat (or zero) through ``horizon_end``."""
    if policy not in ("hold_flat", "drop"):
        raise ValueError(f"cap policy {policy!r}; use 'hold_flat' or 'drop'")
    cap = cap.copy()
    cap["submission_date"] = pd.to_datetime(cap["submission_date"])
    wide = cap.pivot(index="submission_date", columns="country", values="removed_mau").astype(float)
    if wide.index.max() != training_end:
        raise ValueError(f"cap series ends {wide.index.max().date()}, training end is {training_end.date()}")
    future = pd.date_range(training_end + pd.Timedelta(days=1), horizon_end, freq="D")
    last = wide.iloc[-1] if policy == "hold_flat" else wide.iloc[-1] * 0.0
    tail = pd.DataFrame([last.to_numpy()] * len(future), index=future, columns=wide.columns)
    out = pd.concat([wide, tail]).sort_index()
    out.index.name = "date"
    return out


def check_plan_covers(plan: pd.DataFrame, training_end: pd.Timestamp, horizon_end: pd.Timestamp) -> pd.DataFrame:
    plan = plan.copy()
    plan["date"] = pd.to_datetime(plan["date"])
    p = plan.set_index("date").sort_index()
    need = pd.date_range(training_end + pd.Timedelta(days=1), horizon_end, freq="D")
    missing = need.difference(p.index)
    if len(missing):
        raise ValueError(f"paid plan misses {len(missing)} forecast days, first {missing[0].date()}")
    return p.loc[need]


def compose_populations(organic: pd.DataFrame, raw: pd.DataFrame, plan: pd.DataFrame, excess_world: pd.Series,
                        populations: list[str], training_end: pd.Timestamp) -> pd.DataFrame:
    """Per population: organic_<p>, organic_inauthentic_<p>, paid_<p>, total_<p>; plus paid_world_low_90 / high_90 on forecast rows.

    ``organic`` / ``raw`` are curves frames (index date, ``data_type`` + ``mau_<population>``) of the organic build and of the raw
    actuals; ``plan`` is the paid parquet (date, paid_mau, paid_mau_low_90, paid_mau_high_90); ``excess_world`` the cap excess summed
    over countries, history and forecast days (``cap_excess_series``). History: paid = raw − organic − inauthentic."""
    plan = check_plan_covers(plan, training_end, organic.index.max())
    out = pd.DataFrame({"data_type": organic["data_type"]}, index=organic.index)
    hist = organic.index <= training_end
    excess = excess_world.reindex(organic.index).fillna(0.0)
    for pop in populations:
        o = organic[f"mau_{pop}"].astype(float)
        if pop in PAID_POPULATIONS:
            r = raw[f"mau_{pop}"].astype(float).reindex(organic.index)
            inauth = excess.where(o.notna())
            paid_hist = (r - o - inauth).where(hist)
            if (paid_hist.dropna() < -0.5).any():
                raise ValueError(f"{pop}: organic + inauthentic above raw on {int((paid_hist < -0.5).sum())} history days")
            p = paid_hist.fillna(plan["paid_mau"].reindex(organic.index)).where(o.notna())
        else:
            inauth = pd.Series(0.0, index=organic.index).where(o.notna())
            p = pd.Series(0.0, index=organic.index).where(o.notna())
        out[f"organic_{pop}"] = o
        out[f"organic_inauthentic_{pop}"] = inauth
        out[f"paid_{pop}"] = p
        out[f"total_{pop}"] = o + inauth + p
    out["paid_world_low_90"] = plan["paid_mau_low_90"].reindex(organic.index)
    out["paid_world_high_90"] = plan["paid_mau_high_90"].reindex(organic.index)
    out.index.name = "date"
    return out


def combine_bands(composed: pd.DataFrame, organic_low: pd.Series, organic_high: pd.Series) -> pd.DataFrame:
    """Adds organic_world_low/high_90 and total_world_low/high_90 (independent components, half-widths in quadrature)."""
    out = composed.copy()
    o = out["organic_world"]
    o_low, o_high = organic_low.reindex(out.index), organic_high.reindex(out.index)
    p = out["paid_world"]
    p_lo_hw, p_hi_hw = p - out["paid_world_low_90"], out["paid_world_high_90"] - p
    out["organic_world_low_90"], out["organic_world_high_90"] = o_low, o_high
    # the inauthentic column is a fixed level (held flat), so it widens nothing
    out["total_world_low_90"] = out["total_world"] - np.sqrt((o - o_low) ** 2 + p_lo_hw ** 2)
    out["total_world_high_90"] = out["total_world"] + np.sqrt((o_high - o) ** 2 + p_hi_hw ** 2)
    return out


def compose_by_country(organic_bc: pd.DataFrame, raw_bc: pd.DataFrame, plan_paid: pd.Series, shares: pd.Series,
                       excess_bc: pd.DataFrame, training_end: pd.Timestamp) -> pd.DataFrame:
    """Date x country totals (all apps): raw actuals on history days; organic + share x plan paid + own cap excess after."""
    if abs(float(shares.sum()) - 1.0) > 1e-6:
        raise ValueError(f"country paid shares sum to {shares.sum():.6f}, not 1")
    unknown = sorted(set(shares.index) - set(organic_bc.columns))
    if unknown:
        raise ValueError(f"share countries not in the organic frame: {unknown}")
    out = organic_bc.astype(float).copy()
    hist = out.index <= training_end
    paid_fc = plan_paid.reindex(out.index)
    for country in out.columns:
        fc_add = paid_fc * float(shares.get(country, 0.0))
        if country in excess_bc.columns:
            fc_add = fc_add + excess_bc[country].reindex(out.index).fillna(0.0)
        composed = out[country] + fc_add.fillna(0.0)
        composed[hist] = raw_bc[country].astype(float).reindex(out.index)[hist]
        out[country] = composed.where(out[country].notna())
    return out
