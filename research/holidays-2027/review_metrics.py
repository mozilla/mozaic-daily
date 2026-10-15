"""Pure computations behind the 2027 holiday review: world impacts, raw dips, cluster scoring.

Inputs are the frames written by ``extract_holiday_effects.py``, ``run_counterfactual_fits.py``
and ``run_holiday_backtest.py``; nothing here reads files or fits models.

*Raw dip* is model-free on purpose: ``observed / mean(observed at t-28, t-21, t+28, t+35) - 1``,
same weekday, far enough out that a ~10-day holiday period does not contaminate its own baseline.
It sees dips mozaic's detrend never labels (ROW's Eid, Lebaran days past the radius).
"""

import numpy as np
import pandas as pd

from holiday_calendar import cluster_novelty

BASELINE_OFFSETS = (-28, -21, 28, 35)
CLUSTER_PAD_DAYS = 3
KPI_DATE = {2026: "2026-12-15", 2027: "2027-12-15"}
KPI_WINDOW_DAYS = 28

# Tier rule for 2027 clusters (person-days = world DAU the model removes over the cluster span).
HIGH_PERSON_DAYS = 3e6
MEDIUM_PERSON_DAYS = 1e6


def country_base(tile_effects: pd.DataFrame) -> pd.DataFrame:
    """Detrended forecast per (metric, country, date), summed over OS tiles."""
    return tile_effects.groupby(["metric", "country", "date"], as_index=False)["forecast_detrended"].sum()


def country_impacts(effects: pd.DataFrame, base: pd.DataFrame) -> pd.DataFrame:
    """People removed per (metric, country, variant, date): effect x detrended forecast."""
    merged = effects.merge(base, on=["metric", "country", "date"])
    return merged.assign(impact=merged["proportional_effect"] * merged["forecast_detrended"])


def world_impact(impacts: pd.DataFrame, metric: str, variant: str = "as_built") -> pd.Series:
    rows = impacts[(impacts["metric"] == metric) & (impacts["variant"] == variant)]
    return rows.groupby("date")["impact"].sum()


def kpi_window_effect(world: pd.Series, year: int) -> float:
    """Mean holiday impact over the trailing 28 days ending on that year's Dec-15 (the 28d-MA KPI)."""
    end = pd.Timestamp(KPI_DATE[year])
    window = world[(world.index > end - pd.Timedelta(days=KPI_WINDOW_DAYS)) & (world.index <= end)]
    return float(window.mean())


def raw_dip_series(history: pd.DataFrame, metric: str, country: str) -> pd.Series:
    observed = history[(history["metric"] == metric) & (history["country"] == country)].set_index("date")["observed"]
    baseline = np.mean([observed.shift(-offset, freq="D").reindex(observed.index) for offset in BASELINE_OFFSETS], axis=0)
    return observed / baseline - 1


def aligned(series: pd.Series, anchor: pd.Timestamp, offsets: range) -> pd.Series:
    return pd.Series([series.get(anchor + pd.Timedelta(days=k), np.nan) for k in offsets], index=list(offsets))


def holdout_world(backtest: pd.DataFrame, metric: str, variant: str = "as_built") -> pd.DataFrame:
    """Predicted vs realized world holiday impact (people) per holdout day."""
    rows = backtest[(backtest["metric"] == metric) & (backtest["variant"] == variant)]
    rows = rows.assign(
        predicted=rows["predicted_effect"] * rows["expected"],
        realized=rows["realized_effect"] * rows["expected"],
    )
    return rows.groupby("date")[["predicted", "realized"]].sum()


def holdout_window(backtest: pd.DataFrame, country: str, start: str, end: str, variant: str = "as_built") -> dict:
    """Predicted and realized person-days for one country (or world when ``country`` is None) over a span."""
    rows = backtest[(backtest["metric"] == "DAU") & (backtest["variant"] == variant)]
    rows = rows[(rows["date"] >= start) & (rows["date"] <= end)]
    if country:
        rows = rows[rows["country"] == country]
    predicted = float((rows["predicted_effect"] * rows["expected"]).sum())
    realized = float((rows["realized_effect"] * rows["expected"]).sum())
    return {"predicted": predicted, "realized": realized, "ratio": predicted / realized if realized else np.nan}


def score_clusters(clusters: pd.DataFrame, tagged: pd.DataFrame, impacts: pd.DataFrame, shares: pd.Series) -> pd.DataFrame:
    """One row per 2027 cluster: person-days the model removes, what is new, and a tier."""
    dau = impacts[(impacts["metric"] == "DAU") & (impacts["variant"] == "as_built")]
    rows = []
    for cluster in clusters.to_dict("records"):
        start = cluster["start"] - pd.Timedelta(days=CLUSTER_PAD_DAYS)
        end = cluster["end"] + pd.Timedelta(days=CLUSTER_PAD_DAYS)
        span = dau[(dau["country"] == cluster["country"]) & dau["date"].between(start, end)]
        person_days = -float(span["impact"].sum())
        pairs = cluster_novelty(tagged, cluster)
        new_pairs = [p for p in pairs if not p["seen_years"]]
        native_new = [p for p in new_pairs if _both_native(cluster, p)]
        rows.append({
            "country": cluster["country"],
            "share": float(shares.get(cluster["country"], np.nan)),
            "start": cluster["start"], "end": cluster["end"],
            "labels": cluster["labels"],
            "person_days": person_days,
            "peak_effect": float(span["proportional_effect"].min()) if len(span) else 0.0,
            "pairs": pairs, "new_pairs": new_pairs, "native_new_pairs": native_new,
            "tier": _tier(person_days, bool(native_new), bool(new_pairs)),
        })
    return pd.DataFrame(rows).sort_values(["tier", "person_days"], ascending=[True, False]).reset_index(drop=True)


def _both_native(cluster: dict, pair: dict) -> bool:
    sources = {name: source for _, name, _, source in cluster["labels"]}
    return sources.get(pair["a"]) == "native" and sources.get(pair["b"]) == "native"


def _tier(person_days: float, native_new: bool, any_new: bool) -> str:
    """1 = high, 2 = medium, 3 = low. Newness only counts fully when both holidays are real local ones."""
    if native_new and person_days >= HIGH_PERSON_DAYS:
        return "1-high"
    if (native_new and person_days >= MEDIUM_PERSON_DAYS) or (any_new and person_days >= HIGH_PERSON_DAYS):
        return "2-medium"
    return "3-low"
