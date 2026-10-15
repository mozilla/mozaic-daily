"""Pure calendar analysis for the 2027 holiday review: which 2027 holidays crowd each other,
and whether training ever showed the model that arrangement.

Works on ``data/calendar.csv`` (the labels mozaic's ``get_calendar`` builds per country tile).
No model, no I/O beyond what callers pass in.

Vocabulary used across the review:
- *label*      one holiday name on one date in one country (a calendar row may carry several,
               joined with "; "); exploded here to one row per label.
- *moving*     a label whose calendar date wanders more than ``MOVING_SPREAD_DAYS`` across years
               (lunar, lunisolar, Easter-based). Weekend "observed" shifts stay below that.
- *add-on*     a label mozaic adds on top of the country's own ``holidays`` calendar
               (Global, Christian-cycle, outage). ``native`` labels come from the country library.
- *cluster*    2027 labels in one country chained by gaps of at most ``FIT_WINDOW_DAYS``, the
               window inside which mozaic's fit splits a day's dip between nearby labels.
"""

from itertools import combinations

import holidays
import pandas as pd

FIT_WINDOW_DAYS = 7          # Mozaic._fit_holiday_effects(window=7)
MOVING_SPREAD_DAYS = 4
TRAINING_YEARS = range(2021, 2027)
SEEN_TOLERANCE_DAYS = 1      # a training year "shows" a pair if its offset is within this of 2027's
OUTAGE_LABEL = "Legacy telemetry outage 2025"
GLOBAL_SUFFIX = " (Global)"


def explode_labels(calendar: pd.DataFrame) -> pd.DataFrame:
    """One row per (country, date, name) with the country prefix stripped from the name."""
    rows = calendar.rename(columns={"submission_date": "date"}).copy()
    rows["date"] = pd.to_datetime(rows["date"])
    rows["name"] = rows["holiday"].str.split("; ")
    rows = rows.explode("name")
    rows["name"] = [
        name[len(country) + 1:] if name.startswith(country + " ") else name
        for name, country in zip(rows["name"], rows["country"])
    ]
    rows["year"] = rows["date"].dt.year
    return rows[["country", "date", "year", "name"]].drop_duplicates().reset_index(drop=True)


def native_names(country: str, years: range) -> set:
    """Names the country's own ``holidays`` calendar produces (ROW uses the US one, as mozaic does)."""
    library_country = "US" if country == "ROW" else country
    names = set()
    for name in getattr(holidays, library_country)(years=years).values():
        for part in name.split("; "):
            names.add(part.replace(" (observed)", "").replace(" (estimated)", ""))
    return names


def tag_labels(labels: pd.DataFrame) -> pd.DataFrame:
    """Add ``moving``, ``source`` (native / add-on / outage) and ``global_duplicate`` columns."""
    tagged = labels.copy()
    # Judge "moving" on each year's first day, so a fixed multi-day block (RU New Year Holidays,
    # CN Spring Festival days) is not mistaken for a wandering date.
    first_day = tagged.groupby(["country", "name", "year"])["date"].transform("min").dt.dayofyear
    spread = first_day.groupby([tagged["country"], tagged["name"]]).transform(lambda d: d.max() - d.min())
    tagged["moving"] = spread > MOVING_SPREAD_DAYS

    years = range(int(tagged["year"].min()), int(tagged["year"].max()) + 1)
    native = {country: native_names(country, years) for country in tagged["country"].unique()}
    tagged["source"] = [
        "outage" if OUTAGE_LABEL in name else "native" if name in native[country] else "add-on"
        for country, name in zip(tagged["country"], tagged["name"])
    ]

    # The same day off counted twice: a Global add-on sharing its day with any other label
    # ("New Year's Day" + "New Year's Day (Global)"), or any add-on sharing its day with a native
    # label ("Pasqua di Resurrezione" + Christian-cycle "Easter Sunday").
    by_day = tagged.groupby(["country", "date"])
    labels_that_day = by_day["name"].transform("size")
    native_that_day = by_day["source"].transform(lambda s: (s == "native").any())
    is_global = tagged["name"].str.endswith(GLOBAL_SUFFIX)
    tagged["global_duplicate"] = (is_global & (labels_that_day > 1)) | (
        (tagged["source"] == "add-on") & native_that_day
    )
    return tagged


def clusters_for_year(tagged: pd.DataFrame, year: int = 2027) -> pd.DataFrame:
    """Chain one country-year's labels into clusters; keep those with >=2 names and >=1 moving label."""
    records = []
    for country, rows in tagged[(tagged["year"] == year) & (tagged["source"] != "outage")].groupby("country"):
        rows = rows[~rows["global_duplicate"]].sort_values("date")
        cluster, last_date = [], None
        for row in rows.itertuples():
            if last_date is not None and (row.date - last_date).days > FIT_WINDOW_DAYS:
                records.extend(_close_cluster(country, cluster))
                cluster = []
            cluster.append(row)
            last_date = row.date
        records.extend(_close_cluster(country, cluster))
    return pd.DataFrame(records)


def _close_cluster(country: str, cluster: list) -> list:
    names = {row.name for row in cluster}
    if len(names) < 2 or not any(row.moving for row in cluster):
        return []
    return [{
        "country": country,
        "start": min(row.date for row in cluster),
        "end": max(row.date for row in cluster),
        "labels": [(row.date, row.name, row.moving, row.source) for row in cluster],
    }]


def pair_history(tagged: pd.DataFrame, country: str, name_a: str, name_b: str) -> dict:
    """Signed offset (b - a, days) between two labels' nearest block starts in each training year."""
    rows = tagged[tagged["country"] == country].sort_values(["name", "date"])
    continues_block = rows["name"].eq(rows["name"].shift(1)) & (rows["date"] - rows["date"].shift(1)).dt.days.eq(1)
    rows = rows[~continues_block]
    offsets = {}
    for year in TRAINING_YEARS:
        dates_a = rows[(rows["name"] == name_a) & (rows["year"] == year)]["date"]
        dates_b = rows[(rows["name"] == name_b) & (rows["year"] == year)]["date"]
        if dates_a.empty or dates_b.empty:
            continue
        offsets[year] = min(((b - a).days for a in dates_a for b in dates_b), key=abs)
    return offsets


def cluster_novelty(tagged: pd.DataFrame, cluster: dict) -> list:
    """For each close pair in a 2027 cluster with a moving member: 2027 offset and the training years that showed it."""
    pairs = []
    first_days, seen_names = [], set()
    for label in cluster["labels"]:  # one entry per holiday: its first day in the cluster
        if label[1] not in seen_names:
            first_days.append(label)
            seen_names.add(label[1])
    for (date_a, name_a, moving_a, _), (date_b, name_b, moving_b, _) in combinations(first_days, 2):
        if name_a == name_b or not (moving_a or moving_b):
            continue
        offset_2027 = (date_b - date_a).days
        if abs(offset_2027) > FIT_WINDOW_DAYS:
            continue
        history = pair_history(tagged, cluster["country"], name_a, name_b)
        seen = [y for y, off in history.items() if abs(off - offset_2027) <= SEEN_TOLERANCE_DAYS]
        near = [y for y, off in history.items() if abs(off) <= FIT_WINDOW_DAYS]
        pairs.append({
            "a": name_a, "b": name_b, "offset_2027": offset_2027,
            "history": history, "seen_years": seen, "near_years": near,
        })
    return pairs


def same_day_stacks(tagged: pd.DataFrame, year: int = 2027) -> pd.DataFrame:
    """Days in ``year`` carrying two or more distinct labels (exact Global duplicates excluded)."""
    rows = tagged[(tagged["year"] == year) & ~tagged["global_duplicate"] & (tagged["source"] != "outage")]
    stacked = rows.groupby(["country", "date"])["name"].agg(list).reset_index()
    return stacked[stacked["name"].map(len) >= 2].reset_index(drop=True)
