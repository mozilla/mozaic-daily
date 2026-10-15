"""Compute every number and draw every plot in the 2027 holiday review.

Reads ``data/`` (from the extract / counterfactual / backtest scripts), writes
``data/facts.json`` + ``data/clusters_2027.csv`` and ``plots/*.png``. ``render_holiday_review.py``
turns those into ``index.html``; prose numbers come from ``facts.json`` only.
"""

import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import FuncFormatter

from holiday_calendar import clusters_for_year, explode_labels, same_day_stacks, tag_labels
from review_metrics import (aligned, country_base, country_impacts, holdout_window, holdout_world,
                            kpi_window_effect, raw_dip_series, score_clusters, world_impact)

ROOT = Path(__file__).parent
DATA, PLOTS = ROOT / "data", ROOT / "plots"
EID_AL_FITR = {2023: "2023-04-22", 2024: "2024-04-10", 2025: "2025-03-31", 2026: "2026-03-20", 2027: "2027-03-09"}
EID_AL_ADHA = {2023: "2023-06-29", 2024: "2024-06-17", 2025: "2025-06-06", 2026: "2026-05-27", 2027: "2027-05-16"}
ALIGN = range(-6, 13)
TIER_COLOURS = {"1-high": "#c0392b", "2-medium": "#e08e0b", "3-low": "#9aa5b1"}
YEAR_COLOURS = {2023: "#b8c4d6", 2024: "#8fa3c0", 2025: "#5f7ea8", 2026: "#2f5590"}


def load() -> dict:
    read = lambda name: pd.read_csv(DATA / name, parse_dates=["date"])
    frames = {n: read(f"{n}.csv") for n in ["tile_effects", "counterfactual_effects", "history", "backtest_effects"]}
    frames["calendar"] = pd.read_csv(DATA / "calendar.csv")
    return frames


def millions(precision: int):
    return FuncFormatter(lambda v, _: f"{v / 1e6:.{precision}f}M")


def percent(precision: int = 0):
    return FuncFormatter(lambda v, _: f"{v * 100:.{precision}f}%")


def save(fig, name: str) -> str:
    PLOTS.mkdir(exist_ok=True)
    path = PLOTS / name
    fig.savefig(path, dpi=130, bbox_inches="tight")
    plt.close(fig)
    return f"plots/{name}"


def plot_kpi_window(dau: pd.Series, mau: pd.Series) -> str:
    fig, axes = plt.subplots(1, 2, figsize=(13, 3.8))
    for ax, series, label in [(axes[0], dau, "DAU"), (axes[1], mau, "MAU")]:
        for year, style in [(2026, ":"), (2027, "-")]:
            span = series[(series.index >= f"{year}-11-01") & (series.index <= f"{year}-12-31")]
            ax.plot(span.index.map(lambda d: d.replace(year=2027)), span.values, style, color="#2f5590", label=f"{year} forecast")
        ax.axvspan(pd.Timestamp("2027-11-18"), pd.Timestamp("2027-12-15"), color="#f4d58d", alpha=0.4, label="28 days to Dec-15")
        ax.set_title(f"World {label}: people the model removes for holidays")
        ax.yaxis.set_major_formatter(millions(1 if label == "DAU" else 2))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO, interval=1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.tick_params(axis="x", labelrotation=45, labelsize=8)
        ax.legend(fontsize=8)
    return save(fig, "kpi_window.png")


def plot_world_year(dau: pd.Series, clusters: pd.DataFrame) -> str:
    year = dau[dau.index.year == 2027]
    fig = plt.figure(figsize=(14, 7.5))
    top = fig.add_subplot(2, 1, 1)
    top.plot(year.index, year.values, color="#2f5590", lw=0.9)
    for row in clusters[clusters["tier"] != "3-low"].itertuples():
        top.axvspan(row.start, row.end + pd.Timedelta(days=1), color=TIER_COLOURS[row.tier], alpha=0.15)
    top.set_title("2027 world DAU removed for holidays (as built); shaded = medium/high collision clusters")
    top.yaxis.set_major_formatter(millions(0))
    top.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    zooms = [("2027-02-28", "2027-04-04", "Lebaran → Easter"), ("2027-04-26", "2027-06-08", "May / Ascension / Eid al-Adha / Whit"),
             ("2027-12-18", "2027-12-31", "Christmas → New Year")]
    for i, (start, end, title) in enumerate(zooms):
        ax = fig.add_subplot(2, 3, 4 + i)
        span = year[start:end]
        ax.bar(span.index, span.values, color="#2f5590", width=0.8)
        ax.set_title(title, fontsize=10)
        ax.yaxis.set_major_formatter(millions(1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.tick_params(axis="x", labelrotation=45, labelsize=8)
    fig.tight_layout()
    return save(fig, "world_2027.png")


def plot_holdout(by_variant: dict) -> str:
    built = by_variant["as_built"]
    fig = plt.figure(figsize=(14, 7))
    top = fig.add_subplot(2, 1, 1)
    top.plot(built.index, built["realized"], color="#222", lw=0.9, label="realized (observed vs detrended)")
    top.plot(built.index, built["predicted"], color="#c0392b", lw=0.9, alpha=0.8, label="predicted, fit on data before 2025-12-01")
    top.set_title("Holdout: world DAU holiday effect, Dec 2025 → Oct 2026 (people)")
    top.yaxis.set_major_formatter(millions(0))
    top.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    top.legend(fontsize=8)
    zooms = [("2025-12-18", "2026-01-08", "Christmas → New Year"), ("2026-03-14", "2026-04-12", "Lebaran → Easter"),
             ("2026-04-28", "2026-06-06", "May")]
    for i, (start, end, title) in enumerate(zooms):
        ax = fig.add_subplot(2, 3, 4 + i)
        ax.plot(built[start:end].index, built[start:end]["realized"], color="#222", lw=1.4, label="realized")
        ax.plot(built[start:end].index, built[start:end]["predicted"], color="#c0392b", lw=1.2, label="as built")
        no_dup = by_variant["no_duplicates"][start:end]["predicted"]
        ax.plot(no_dup.index, no_dup.values, color="#e08e0b", lw=1, ls="--", label="duplicates removed")
        ax.set_title(title, fontsize=10)
        ax.yaxis.set_major_formatter(millions(1))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
        ax.tick_params(axis="x", labelrotation=45, labelsize=8)
        ax.legend(fontsize=7)
    fig.tight_layout()
    return save(fig, "holdout.png")


def plot_aligned_dips(history, effects, panels, name, title) -> str:
    """Raw dips per training year aligned on a moving holiday, with the model's 2027 effect."""
    fig, axes = plt.subplots(1, len(panels), figsize=(6.5 * len(panels), 4), sharey=True)
    for ax, (country, holiday_name, dates) in zip(np.atleast_1d(axes), panels):
        dips = raw_dip_series(history, "DAU", country)
        for year, day in dates.items():
            if year == 2027:
                continue
            ax.plot(list(ALIGN), aligned(dips, pd.Timestamp(day), ALIGN).values, color=YEAR_COLOURS[year], lw=1.3, label=f"{year} actual dip")
        model = effects[(effects["country"] == country) & (effects["metric"] == "DAU") & (effects["variant"] == "as_built")]
        ax.plot(list(ALIGN), aligned(model.set_index("date")["proportional_effect"], pd.Timestamp(dates[2027]), ALIGN).values,
                color="#c0392b", lw=2.4, label="2027 model effect")
        ax.axhline(0, color="#999", lw=0.6)
        ax.axvline(0, color="#999", lw=0.6, ls=":")
        ax.set_title(f"{country}: days from {holiday_name}")
        ax.yaxis.set_major_formatter(percent())
        ax.legend(fontsize=8)
    fig.suptitle(title)
    fig.tight_layout()
    return save(fig, name)


def plot_country_strips(effects, backtest, tagged, countries, start_2027, end_2027, name, title, panel_width=3.6) -> str:
    """Row 1: 2026 holdout (predicted vs realized) for the same season. Row 2: 2027 predicted with labels."""
    fig, axes = plt.subplots(2, len(countries), figsize=(panel_width * len(countries), 6.2), sharey="row", squeeze=False)
    for col, country in enumerate(countries):
        held = backtest[(backtest["country"] == country) & (backtest["metric"] == "DAU") & (backtest["variant"] == "as_built")].set_index("date")
        span_2026 = held[start_2027.replace("2027", "2026"):end_2027.replace("2027", "2026")]
        axes[0, col].plot(span_2026.index, span_2026["realized_effect"], color="#222", lw=1.2, label="2026 realized")
        axes[0, col].plot(span_2026.index, span_2026["predicted_effect"], color="#c0392b", lw=1.1, label="2026 predicted")
        model = effects[(effects["country"] == country) & (effects["metric"] == "DAU") & (effects["variant"] == "as_built")].set_index("date")
        span_2027 = model[start_2027:end_2027]["proportional_effect"]
        axes[1, col].bar(span_2027.index, span_2027.values, color="#2f5590", width=0.8)
        labels = tagged[(tagged["country"] == country) & tagged["date"].between(start_2027, end_2027) & ~tagged["global_duplicate"]]
        for row in labels.itertuples():
            axes[1, col].annotate(row.name[:22], (row.date, 0), xytext=(0, 4), textcoords="offset points", rotation=90,
                                  fontsize=6.5, color="#c0392b" if row.source != "native" else "#222", va="bottom", ha="center")
        axes[0, col].set_title(country)
        for ax in axes[:, col]:
            ax.yaxis.set_major_formatter(percent())
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
            ax.tick_params(axis="x", labelrotation=45, labelsize=7)
        axes[1, col].set_ylim(top=0.45)
    axes[0, 0].legend(fontsize=7)
    axes[0, 0].set_ylabel("2026 holdout")
    axes[1, 0].set_ylabel("2027 model (red text = mozaic add-on label)")
    fig.suptitle(title)
    fig.tight_layout()
    return save(fig, name)


def unlabeled_eid_person_days(history, base, country: str, dates: dict) -> dict:
    dips = raw_dip_series(history, "DAU", country)
    mean_dip = pd.concat([aligned(dips, pd.Timestamp(d), range(-2, 5)) for y, d in dates.items() if y < 2027], axis=1).mean(axis=1)
    level = base[(base["metric"] == "DAU") & (base["country"] == country)].set_index("date")["forecast_detrended"]
    anchor = pd.Timestamp(dates[2027])
    people = [mean_dip[k] * level.get(anchor + pd.Timedelta(days=k), np.nan) for k in mean_dip.index]
    return {"ROW-days": float(mean_dip.sum()), "person_days": float(np.nansum(people)), "peak_people": float(np.nanmin(people))}


def phantom_label_effects(impacts, tagged, name: str, date: str) -> dict:
    """Effect and people on ``date`` in countries where ``name`` is the only label that day (a mozaic add-on)."""
    day = pd.Timestamp(date)
    labelled = tagged[(tagged["date"] == day)]
    alone = [c for c, g in labelled.groupby("country") if set(g["name"]) == {name}]
    rows = impacts[(impacts["metric"] == "DAU") & (impacts["variant"] == "as_built") & (impacts["date"] == day)]
    rows = rows[rows["country"].isin(alone)]
    return {r.country: {"effect": float(r.proportional_effect), "people": float(r.impact)} for r in rows.itertuples()}


def main() -> None:
    frames = load()
    effects, history, backtest = frames["counterfactual_effects"], frames["history"], frames["backtest_effects"]
    base = country_base(frames["tile_effects"])
    impacts = country_impacts(effects, base)
    tagged = tag_labels(explode_labels(frames["calendar"]))
    dau_2027 = base[(base["metric"] == "DAU") & (base["date"].dt.year == 2027)]
    shares = dau_2027.groupby("country")["forecast_detrended"].sum() / dau_2027["forecast_detrended"].sum()
    clusters = score_clusters(clusters_for_year(tagged), tagged, impacts, shares)

    world = {(m, v): world_impact(impacts, m, v) for m in ["DAU", "MAU"] for v in ["as_built", "no_duplicates", "no_outage_label", "both"]}
    held = {v: holdout_world(backtest, "DAU", v) for v in ["as_built", "no_duplicates", "no_outage_label", "both"]}
    held_error = {
        (m, v): float(((backtest[(backtest.metric == m) & (backtest.variant == v)].eval("(predicted_effect - realized_effect) * expected"))
                       .groupby(backtest["date"]).sum()).abs().sum())
        for m in ["DAU", "MAU"] for v in ["as_built", "no_duplicates", "no_outage_label", "both"]
    }
    world_level = base[base["metric"] == "DAU"].groupby("date")["forecast_detrended"].sum()

    facts = {
        "shares": shares.round(4).to_dict(),
        "world_dau_2027_mean": float(world_level[world_level.index.year == 2027].mean()),
        "kpi": {str(y): {"dau_28d": kpi_window_effect(world[("DAU", "as_built")], y),
                         "dau_28d_no_outage": kpi_window_effect(world[("DAU", "no_outage_label")], y),
                         "mau_dec15": float(world[("MAU", "as_built")].get(pd.Timestamp(f"{y}-12-15"), 0.0))} for y in (2026, 2027)},
        "annual_person_days": {m: -float(world[(m, "as_built")][world[(m, "as_built")].index.year == 2027].sum()) for m in ["DAU", "MAU"]},
        "duplicate_delta_person_days": {m: float((world[(m, "no_duplicates")] - world[(m, "as_built")])["2027"].sum()) for m in ["DAU", "MAU"]},
        "outage_delta_person_days": {m: float((world[(m, "no_outage_label")] - world[(m, "as_built")])["2027"].sum()) for m in ["DAU", "MAU"]},
        "jan1_2027": {v: float(world[("DAU", v)].get(pd.Timestamp("2027-01-01"))) for v in ["as_built", "no_duplicates"]},
        "holdout_abs_error": {f"{m}|{v}": e for (m, v), e in held_error.items()},
        "holdout_windows": {
            name: holdout_window(backtest, c, a, z) for name, c, a, z in [
                ("world_xmas_ny", None, "2025-12-18", "2026-01-10"), ("id_lebaran", "ID", "2026-03-13", "2026-03-31"),
                ("id_late_may", "ID", "2026-05-20", "2026-06-08"), ("cn_spring", "CN", "2026-02-10", "2026-02-28"),
                ("de_easter", "DE", "2026-03-29", "2026-04-10"), ("row_easter", "ROW", "2026-03-29", "2026-04-10"),
                ("fr_may", "FR", "2026-04-28", "2026-05-30"), ("de_may", "DE", "2026-04-28", "2026-05-30"),
                ("pl_may", "PL", "2026-04-28", "2026-05-30"), ("row_may", "ROW", "2026-04-28", "2026-05-30"),
                ("us_memorial_whit", "US", "2026-05-20", "2026-05-30"), ("in_holi_eid", "IN", "2026-03-01", "2026-03-25"),
            ]},
        "unlabeled_eid_row": {"fitr": unlabeled_eid_person_days(history, base, "ROW", EID_AL_FITR),
                              "adha": unlabeled_eid_person_days(history, base, "ROW", EID_AL_ADHA)},
        "lebaran": {
            "history_id_days": float(pd.concat([aligned(raw_dip_series(history, "DAU", "ID"), pd.Timestamp(d), ALIGN)
                                                for y, d in EID_AL_FITR.items() if y < 2027], axis=1).mean(axis=1).sum()),
            "model_id_days": float(aligned(effects[(effects.country == "ID") & (effects.metric == "DAU") & (effects.variant == "as_built")]
                                           .set_index("date")["proportional_effect"], pd.Timestamp(EID_AL_FITR[2027]), ALIGN).sum()),
            "id_level": float(base[(base.metric == "DAU") & (base.country == "ID")].set_index("date")["forecast_detrended"]["2027-03"].mean()),
        },
        "phantom_whit_2027": phantom_label_effects(impacts, tagged, "Whit Monday (Global)", "2027-05-17"),
        "stacks_2027": same_day_stacks(tagged).assign(date=lambda f: f["date"].dt.strftime("%Y-%m-%d")).to_dict("records"),
        "plots": {
            "kpi": plot_kpi_window(world[("DAU", "as_built")], world[("MAU", "as_built")]),
            "world": plot_world_year(world[("DAU", "as_built")], clusters),
            "holdout": plot_holdout(held),
            "lebaran": plot_aligned_dips(history, effects, [("ID", "Eid al-Fitr", EID_AL_FITR)], "lebaran.png",
                                         "Indonesia: desktop DAU dip around Eid al-Fitr (Lebaran), by year, vs the 2027 model"),
            "row_eid": plot_aligned_dips(history, effects, [("ROW", "Eid al-Fitr", EID_AL_FITR), ("ROW", "Eid al-Adha", EID_AL_ADHA)],
                                         "row_eid.png", "Rest of world: the Eid dips the ROW calendar has no label for"),
            "may": plot_country_strips(effects, backtest, tagged, ["DE", "FR", "PL", "ROW", "US", "ID"], "2027-04-26", "2027-06-08",
                                       "may.png", "May: 2026 holdout (top) and the 2027 arrangement the model will apply (bottom)"),
            "indonesia": plot_country_strips(effects, backtest, tagged, ["ID"], "2027-01-01", "2027-12-31",
                                             "indonesia_year.png", "Indonesia: 2026 holdout (top) and every 2027 holiday effect (bottom)",
                                             panel_width=15),
            "easter": plot_country_strips(effects, backtest, tagged, ["DE", "FR", "ROW", "IN", "ID"], "2027-03-01", "2027-04-10",
                                          "easter.png", "March–April: 2026 holdout (top) and 2027 (bottom; Easter 2027 = Mar 28)"),
        },
    }
    clusters.to_csv(DATA / "clusters_2027.csv", index=False)
    (DATA / "facts.json").write_text(json.dumps(facts, indent=1, default=str))
    print(json.dumps({k: v for k, v in facts.items() if k not in ("stacks_2027", "shares")}, indent=1, default=str))


if __name__ == "__main__":
    main()
