# -*- coding: utf-8 -*-
"""Tests for the mobile paid/organic split — adjustment ``p``.

Covers both halves:

- ``mozaic_daily.organic_source`` — building the measured split artifact (producer side).
- ``mozaic_daily.organic`` — applying it around a mozaic run (consumer side).

The fixture is a 3-country x 4-app x 90-day mobile training frame whose Fenix organic share
*ramps* over time and *differs sharply by country*, because both are true of the real data
(1.10% paid in 2024-06 rising to 11.58% by 2026-07; 0.2% in RU vs 27.6% in ID) and both are
exactly what a naive implementation would get wrong.

The measured split deliberately starts partway through the training window, so every test
exercises the held-flat backfill the way production does.
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from mozaic_daily.organic import (
    add_paid_to_forecast,
    build_share_lookup,
    load_organic_spec,
    load_split_frame,
    marketing_paid_level,
    measured_paid_country_shares,
    paid_seam_step,
    split_training_to_organic,
)
from mozaic_daily.organic_source import (
    build_split_frame,
    check_partition_identity,
    check_shredder_drift,
    check_split_coverage,
    check_tail_overlap,
    combine_snapshot_and_tail,
    production_fenix_daily,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

APP_FLAGS = ["fenix_android", "firefox_ios", "focus_android", "focus_ios"]
TEST_COUNTRIES = ["US", "ID", "IR"]
TEST_DATES = pd.date_range("2026-01-01", "2026-03-31", freq="D")
TRAINING_END = pd.Timestamp("2026-03-31")
FORECAST_START = pd.Timestamp("2026-04-01")

#: The split only covers the back half of the training window, so the first 59 days must be
#: filled by holding the earliest measured share flat backwards.
MEASURED_FROM = pd.Timestamp("2026-03-01")

FENIX_BASELINE = {"US": 1_000_000, "ID": 400_000, "IR": 100_000}
OTHER_BASELINE = {"US": 300_000, "ID": 120_000, "IR": 30_000}

#: Organic share on the first measured day and on TRAINING_END, per country. US drifts, ID is
#: heavily paid, IR is nearly all organic — mirroring the real spread.
SHARE_START = {"US": 0.90, "ID": 0.75, "IR": 0.99}
SHARE_END = {"US": 0.88, "ID": 0.72, "IR": 0.98}


def _share_on(country: str, date: pd.Timestamp) -> float:
    """Linear ramp between SHARE_START and SHARE_END over the measured window."""
    span = (TRAINING_END - MEASURED_FROM).days
    frac = (date - MEASURED_FROM).days / span
    return SHARE_START[country] + frac * (SHARE_END[country] - SHARE_START[country])


def _make_training_fixture() -> pd.DataFrame:
    rows = []
    for date in TEST_DATES:
        for country in TEST_COUNTRIES:
            for app in APP_FLAGS:
                baseline = FENIX_BASELINE if app == "fenix_android" else OTHER_BASELINE
                row = {"x": date, "country": country, "y": baseline[country]}
                for flag in APP_FLAGS:
                    row[flag] = (flag == app)
                rows.append(row)
    df = pd.DataFrame(rows)
    df["y"] = df["y"].astype("Int64")
    return df


def _make_split_fixture() -> pd.DataFrame:
    """A measured split covering only MEASURED_FROM..TRAINING_END."""
    rows = []
    for date in pd.date_range(MEASURED_FROM, TRAINING_END, freq="D"):
        for country in TEST_COUNTRIES:
            total = FENIX_BASELINE[country]
            organic = int(round(total * _share_on(country, date)))
            rows.append({
                "submission_date": date,
                "country": country,
                "organic_dau": organic,
                "paid_dau": total - organic,
                "total_dau": total,
                "organic_share": organic / total,
            })
    return pd.DataFrame(rows)


def _make_marketing_lift() -> pd.Series:
    """A flat 200,000 lift from the anchor onward, so level = 200,000 + anchor."""
    idx = pd.date_range("2026-02-01", "2026-06-30", freq="D")
    return pd.Series(200_000.0, index=idx, name="marketing_lift_daily")


@pytest.fixture
def training_df():
    return _make_training_fixture()


@pytest.fixture
def split():
    return _make_split_fixture()


@pytest.fixture
def share_lookup(split):
    return build_share_lookup(
        split,
        share_column="organic_share",
        training_dates=TEST_DATES,
        countries=["US", "ID"],          # IR excluded from the split, as in production
    )


@pytest.fixture
def organic_spec_dir(tmp_path, split):
    """A complete on-disk spec + data pair, so loader tests exercise real files."""
    split.to_parquet(tmp_path / "split.parquet", index=False)
    lift = _make_marketing_lift().to_frame()
    lift.index.name = "target_date"
    lift.to_parquet(tmp_path / "lift.parquet")
    spec = {
        "type": "paid_organic_split",
        "platform": "mobile",
        "data_file": "split.parquet",
        "share_column": "organic_share",
        "scope": {"app_flag_column": "fenix_android", "exclude_countries": ["IR"]},
        "share_backfill": {"policy": "hold_earliest", "measured_from": str(MEASURED_FROM.date())},
        "paid_forecast": {
            "data_file": "lift.parquet",
            "value_column": "marketing_lift_daily",
            "anchor_paid_dau": 800_000.0,
            "tail_policy": "hold_last",
        },
        "allocation": {"key": "trailing_paid_dau_share", "window_days": 28},
        "applies_to_forecast_start": str(FORECAST_START.date()),
    }
    (tmp_path / "organic.json").write_text(json.dumps(spec, indent=2))
    return tmp_path


# =============================================================================
# Producer side — mozaic_daily.organic_source
# =============================================================================

def _mirror_rows(dates, countries, organic, paid_12mo, paid_prior):
    return pd.DataFrame([
        {"submission_date": d, "country": c, "growth_source": gs, "dau": v}
        for d in dates for c in countries
        for gs, v in [("organic", organic), ("paid_rolling_12mo", paid_12mo),
                      ("paid_prior_1yr", paid_prior)]
    ])


def test_combine_prefers_tail_on_overlap_without_double_counting():
    snap = _mirror_rows(pd.date_range("2026-01-01", "2026-01-10"), ["US"], 900, 80, 20)
    tail = _mirror_rows(pd.date_range("2026-01-08", "2026-01-15"), ["US"], 950, 40, 10)
    out = combine_snapshot_and_tail(snap, tail)

    # 15 days x 1 country x 3 growth sources, no duplicates from the 3-day overlap.
    assert len(out) == 15 * 3
    overlap_day = out[out["submission_date"] == pd.Timestamp("2026-01-08")]
    assert overlap_day.set_index("growth_source")["dau"]["organic"] == 950


def test_tail_overlap_check_rejects_a_drifted_tail():
    snap = _mirror_rows(pd.date_range("2026-01-01", "2026-01-10"), ["US"], 900, 80, 20)
    # Tail runs 20% high — e.g. the app_name filter was dropped, pulling in MozillaOnline.
    drifted = _mirror_rows(pd.date_range("2026-01-08", "2026-01-15"), ["US"], 1200, 80, 20)
    with pytest.raises(ValueError, match="drifted from the mirror build"):
        check_tail_overlap(snap, drifted)


def test_tail_overlap_check_rejects_too_short_an_overlap():
    snap = _mirror_rows(pd.date_range("2026-01-01", "2026-01-10"), ["US"], 900, 80, 20)
    tail = _mirror_rows(pd.date_range("2026-01-10", "2026-01-15"), ["US"], 900, 80, 20)
    with pytest.raises(ValueError, match="overlaps the snapshot on only 1 day"):
        check_tail_overlap(snap, tail)


def test_partition_identity_rejects_an_unknown_growth_source():
    """A new bucket would silently vanish from the share denominator."""
    mirror = _mirror_rows(pd.date_range("2026-01-01", "2026-01-05"), ["US"], 900, 80, 20)
    mirror.loc[len(mirror)] = {
        "submission_date": pd.Timestamp("2026-01-01"), "country": "US",
        "growth_source": "paid_tiktok", "dau": 500,
    }
    with pytest.raises(ValueError, match="unknown growth_source"):
        check_partition_identity(mirror)


def test_build_split_frame_computes_share_from_all_three_buckets():
    mirror = _mirror_rows([pd.Timestamp("2026-01-01")], ["US"], 900, 80, 20)
    out = build_split_frame(mirror)
    row = out.iloc[0]
    assert row["organic_dau"] == 900
    assert row["paid_dau"] == 100          # both paid buckets, not just the rolling one
    assert row["total_dau"] == 1000
    assert row["organic_share"] == pytest.approx(0.9)


def test_split_coverage_rejects_a_missing_country():
    mirror = _mirror_rows(pd.date_range("2026-01-01", "2026-01-05"), ["US"], 900, 80, 20)
    split = build_split_frame(mirror)
    with pytest.raises(ValueError, match=r"\['DE'\].*100% organic"):
        check_split_coverage(split, expected_countries={"US", "DE"},
                             training_end=pd.Timestamp("2026-01-05"))


def test_split_coverage_rejects_a_gap_in_the_daily_grid():
    dates = pd.DatetimeIndex(["2026-01-01", "2026-01-02", "2026-01-05"])
    split = build_split_frame(_mirror_rows(dates, ["US"], 900, 80, 20))
    with pytest.raises(ValueError, match="missing day"):
        check_split_coverage(split, expected_countries={"US"},
                             training_end=pd.Timestamp("2026-01-05"))


def test_split_coverage_rejects_a_split_that_stops_before_training_end():
    split = build_split_frame(
        _mirror_rows(pd.date_range("2026-01-01", "2026-01-05"), ["US"], 900, 80, 20))
    with pytest.raises(ValueError, match="extend the tail query"):
        check_split_coverage(split, expected_countries={"US"},
                             training_end=pd.Timestamp("2026-01-31"))


def test_production_fenix_daily_selects_only_fenix(training_df):
    daily = production_fenix_daily(training_df)
    expected = sum(FENIX_BASELINE.values())
    assert daily.loc[pd.Timestamp("2026-01-01")] == expected
    assert len(daily) == len(TEST_DATES)


def test_shredder_drift_rejects_a_nonzero_gap_at_the_trailing_edge():
    """A trailing-edge gap is a population mismatch, not attrition — a much worse failure."""
    dates = pd.date_range("2026-01-01", "2026-02-28")
    split = build_split_frame(_mirror_rows(dates, ["US"], 900, 80, 20))
    # Production consistently 5% above the mirror, including on the newest days.
    production = pd.Series(1050.0, index=dates)
    with pytest.raises(ValueError, match="cover different populations"):
        check_shredder_drift(split, production)


def test_shredder_drift_accepts_attrition_that_closes():
    dates = pd.date_range("2026-01-01", "2026-02-28")
    split = build_split_frame(_mirror_rows(dates, ["US"], 900, 80, 20))
    # Gap decays from -3% to exactly 0 and then STAYS at 0 for the last month — the real shape
    # (deletion requests have not yet been processed against recent days).
    n_closed = 30
    gap = np.concatenate([
        np.linspace(0.03, 0.0, len(dates) - n_closed),
        np.zeros(n_closed),
    ])
    production = pd.Series(1000.0 / (1 - gap), index=dates)
    result = check_shredder_drift(split, production)
    assert result.iloc[0]["status"] == "PASS"
    assert result.iloc[0]["oldest_rel_gap"] < -0.02
    assert abs(result.iloc[0]["newest_rel_gap"]) < 1e-9


# =============================================================================
# Consumer side — the share lookup and backfill
# =============================================================================

def test_share_lookup_holds_the_earliest_measured_share_backwards(share_lookup):
    """Pre-measurement days must take the FIRST measured value, not the last or a mean."""
    first_measured = _share_on("US", MEASURED_FROM)
    for day in ["2026-01-01", "2026-02-14", "2026-02-28"]:
        assert share_lookup[(pd.Timestamp(day), "US")] == pytest.approx(first_measured, abs=1e-6)


def test_share_lookup_uses_measured_values_inside_the_window(share_lookup):
    """Inside the measured window the ramp must be preserved, not flattened."""
    mid = pd.Timestamp("2026-03-15")
    assert share_lookup[(mid, "US")] == pytest.approx(_share_on("US", mid), abs=1e-6)
    assert share_lookup[(mid, "US")] != pytest.approx(_share_on("US", MEASURED_FROM), abs=1e-6)


def test_share_lookup_keeps_countries_distinct(share_lookup):
    """A global share would collapse ID's 25% paid into US's 10%."""
    day = pd.Timestamp("2026-03-15")
    assert share_lookup[(day, "US")] == pytest.approx(_share_on("US", day), abs=1e-6)
    assert share_lookup[(day, "ID")] == pytest.approx(_share_on("ID", day), abs=1e-6)
    assert share_lookup[(day, "US")] - share_lookup[(day, "ID")] > 0.1


def test_share_lookup_raises_for_a_country_with_no_measurement(split):
    with pytest.raises(ValueError, match="no measured organic share for.*BR"):
        build_share_lookup(split, share_column="organic_share",
                           training_dates=TEST_DATES, countries=["US", "BR"])


# =============================================================================
# Consumer side — the training split
# =============================================================================

def test_split_partitions_every_fenix_row_exactly(training_df, share_lookup):
    """organic + measured_paid == y, per row. This is the invariant the add-back relies on."""
    organic_df, measured_paid = split_training_to_organic(
        training_df, share_lookup=share_lookup, exclude_countries=["IR"])
    mask = (training_df["fenix_android"]) & (training_df["country"] != "IR")
    before = training_df.loc[mask].set_index(["x", "country"])["y"].astype("int64")
    after = organic_df.loc[mask].set_index(["x", "country"])["y"].astype("int64")
    recovered = after.add(measured_paid.rename_axis(["x", "country"]), fill_value=0)
    pd.testing.assert_series_equal(
        recovered.astype("int64").sort_index(), before.sort_index(), check_names=False)


def test_split_leaves_non_fenix_apps_untouched(training_df, share_lookup):
    """iOS and Focus carry no paid signal, so they must pass through unchanged."""
    organic_df, _ = split_training_to_organic(
        training_df, share_lookup=share_lookup, exclude_countries=["IR"])
    others = ~training_df["fenix_android"]
    assert (organic_df.loc[others, "y"].to_numpy() == training_df.loc[others, "y"].to_numpy()).all()


def test_split_leaves_excluded_countries_untouched(training_df, share_lookup):
    organic_df, measured_paid = split_training_to_organic(
        training_df, share_lookup=share_lookup, exclude_countries=["IR"])
    ir = training_df["country"] == "IR"
    assert (organic_df.loc[ir, "y"].to_numpy() == training_df.loc[ir, "y"].to_numpy()).all()
    assert "IR" not in measured_paid.index.get_level_values("country")


def test_split_actually_removes_paid(training_df, share_lookup):
    """Guards against a no-op implementation passing every other test."""
    organic_df, measured_paid = split_training_to_organic(
        training_df, share_lookup=share_lookup, exclude_countries=["IR"])
    day = pd.Timestamp("2026-03-31")
    us = (organic_df["x"] == day) & (organic_df["country"] == "US") & organic_df["fenix_android"]
    expected_organic = round(FENIX_BASELINE["US"] * _share_on("US", day))
    assert int(organic_df.loc[us, "y"].iloc[0]) == expected_organic
    assert measured_paid[(day, "US")] == FENIX_BASELINE["US"] - expected_organic
    assert measured_paid[(day, "US")] > 0


def test_split_preserves_int64_dtype(training_df, share_lookup):
    organic_df, _ = split_training_to_organic(
        training_df, share_lookup=share_lookup, exclude_countries=["IR"])
    assert organic_df["y"].dtype == "Int64"


def test_split_does_not_mutate_the_input(training_df, share_lookup):
    before = training_df["y"].copy()
    split_training_to_organic(training_df, share_lookup=share_lookup, exclude_countries=["IR"])
    pd.testing.assert_series_equal(training_df["y"], before)


def test_split_refuses_to_run_twice(training_df, share_lookup):
    organic_df, _ = split_training_to_organic(
        training_df, share_lookup=share_lookup, exclude_countries=["IR"])
    with pytest.raises(RuntimeError, match="double-subtract"):
        split_training_to_organic(organic_df, share_lookup=share_lookup, exclude_countries=["IR"])


def test_split_raises_when_the_lookup_has_a_hole(training_df, split):
    """A silently-missing share would mean 'no paid here', inflating that tile's organic."""
    partial = build_share_lookup(split, share_column="organic_share",
                                 training_dates=TEST_DATES, countries=["US", "ID"])
    partial = partial.drop(index=(pd.Timestamp("2026-03-15"), "US"))
    with pytest.raises(ValueError, match="no organic share"):
        split_training_to_organic(training_df, share_lookup=partial, exclude_countries=["IR"])


# =============================================================================
# Consumer side — allocation, the marketing level, and the add-back
# =============================================================================

def test_paid_shares_follow_paid_dau_not_total_dau(split):
    """The whole point of `trailing_paid_dau_share`.

    US has 2.5x ID's Fenix DAU but ID is ~3x more paid-intensive, so a total-DAU key and a
    paid-DAU key give materially different allocations. Pinning the difference stops a silent
    revert to the `m` behaviour.
    """
    paid_key = measured_paid_country_shares(
        split, training_end_date=TRAINING_END, window_days=28,
        exclude_countries=["IR"], allocation_key="trailing_paid_dau_share")
    total_key = measured_paid_country_shares(
        split, training_end_date=TRAINING_END, window_days=28,
        exclude_countries=["IR"], allocation_key="trailing_dau_share",
        dau_training=_make_training_fixture())

    assert paid_key.sum() == pytest.approx(1.0)
    assert total_key.sum() == pytest.approx(1.0)
    # ID is over-weighted by the paid key relative to the total-DAU key.
    assert paid_key["ID"] > total_key["ID"] + 0.05


def test_paid_shares_exclude_and_renormalize(split):
    shares = measured_paid_country_shares(
        split, training_end_date=TRAINING_END, window_days=28, exclude_countries=["IR"])
    assert "IR" not in shares.index
    assert shares.sum() == pytest.approx(1.0)


def test_paid_shares_use_only_the_trailing_window(split):
    """A country whose paid DAU only exists outside the window must not get a share."""
    extra = split.copy()
    old = extra[extra["submission_date"] == MEASURED_FROM].copy()
    old["country"] = "ZZ"
    narrow = measured_paid_country_shares(
        pd.concat([extra, old], ignore_index=True),
        training_end_date=TRAINING_END, window_days=7, exclude_countries=["IR"])
    assert "ZZ" not in narrow.index


def test_marketing_level_is_lift_plus_anchor(organic_spec_dir):
    spec = load_organic_spec(organic_spec_dir / "organic.json")
    level = marketing_paid_level(spec, organic_spec_dir,
                                 forecast_start=FORECAST_START, forecast_end="2026-06-30")
    assert level.loc[FORECAST_START] == pytest.approx(200_000.0 + 800_000.0)


def test_marketing_level_holds_flat_past_the_curve(organic_spec_dir):
    """Zero-filling here would drop the whole paid level off a cliff on the first uncovered day."""
    spec = load_organic_spec(organic_spec_dir / "organic.json")
    level = marketing_paid_level(spec, organic_spec_dir,
                                 forecast_start=FORECAST_START, forecast_end="2027-12-31")
    last_covered = level.loc[pd.Timestamp("2026-06-30")]
    assert level.loc[pd.Timestamp("2027-12-31")] == pytest.approx(last_covered)
    assert level.min() > 0


def test_marketing_level_requires_an_anchor(organic_spec_dir):
    """Without the anchor every total shifts by a constant with the shape left right."""
    spec = load_organic_spec(organic_spec_dir / "organic.json")
    del spec["paid_forecast"]["anchor_paid_dau"]
    with pytest.raises(ValueError, match="anchor_paid_dau is required"):
        marketing_paid_level(spec, organic_spec_dir,
                             forecast_start=FORECAST_START, forecast_end="2026-06-30")


def test_marketing_level_rejects_a_curve_that_starts_too_late(organic_spec_dir):
    spec = load_organic_spec(organic_spec_dir / "organic.json")
    with pytest.raises(ValueError, match="after forecast_start"):
        marketing_paid_level(spec, organic_spec_dir,
                             forecast_start=pd.Timestamp("2026-01-01"), forecast_end="2026-06-30")


def _forecast_frame():
    """A minimal post-combine_tables frame: per-country and ALL rows, both regions."""
    rows = []
    dates = [pd.Timestamp("2026-03-31"), FORECAST_START]
    for date in dates:
        source = "actual" if date < FORECAST_START else "forecast"
        for country in ["US", "ID", "ALL"]:
            for population in ["fenix_android", "firefox_ios", "ALL"]:
                rows.append({"target_date": date, "country": country,
                             "population": population, "source": source, "DAU": 1_000_000.0})
    return pd.DataFrame(rows)


@pytest.fixture
def add_back_inputs(training_df, share_lookup, split, organic_spec_dir):
    _, measured_paid = split_training_to_organic(
        training_df, share_lookup=share_lookup, exclude_countries=["IR"])
    spec = load_organic_spec(organic_spec_dir / "organic.json")
    marketing = marketing_paid_level(spec, organic_spec_dir,
                                     forecast_start=FORECAST_START, forecast_end="2026-06-30")
    shares = measured_paid_country_shares(
        split, training_end_date=TRAINING_END, window_days=28, exclude_countries=["IR"])
    return measured_paid, marketing, shares


def test_training_rows_get_the_measured_paid_back(add_back_inputs):
    """The invariant verify_training_rows_are_actuals.py enforces."""
    measured_paid, marketing, shares = add_back_inputs
    out = add_paid_to_forecast(_forecast_frame(), measured_paid=measured_paid,
                               marketing_paid=marketing, country_shares=shares,
                               forecast_start=FORECAST_START)
    day = pd.Timestamp("2026-03-31")
    row = out[(out["target_date"] == day) & (out["country"] == "US")
              & (out["population"] == "fenix_android")]
    assert row["DAU"].iloc[0] == pytest.approx(1_000_000.0 + measured_paid[(day, "US")])


def test_forecast_rows_get_marketing_level_by_share(add_back_inputs):
    measured_paid, marketing, shares = add_back_inputs
    out = add_paid_to_forecast(_forecast_frame(), measured_paid=measured_paid,
                               marketing_paid=marketing, country_shares=shares,
                               forecast_start=FORECAST_START)
    row = out[(out["target_date"] == FORECAST_START) & (out["country"] == "ID")
              & (out["population"] == "fenix_android")]
    expected = 1_000_000.0 + marketing.loc[FORECAST_START] * shares["ID"]
    assert row["DAU"].iloc[0] == pytest.approx(expected)


def test_world_row_gets_the_full_level_not_a_share(add_back_inputs):
    measured_paid, marketing, shares = add_back_inputs
    out = add_paid_to_forecast(_forecast_frame(), measured_paid=measured_paid,
                               marketing_paid=marketing, country_shares=shares,
                               forecast_start=FORECAST_START)
    row = out[(out["target_date"] == FORECAST_START) & (out["country"] == "ALL")
              & (out["population"] == "ALL")]
    assert row["DAU"].iloc[0] == pytest.approx(1_000_000.0 + marketing.loc[FORECAST_START])


def test_world_training_row_gets_the_summed_measured_paid(add_back_inputs):
    measured_paid, marketing, shares = add_back_inputs
    out = add_paid_to_forecast(_forecast_frame(), measured_paid=measured_paid,
                               marketing_paid=marketing, country_shares=shares,
                               forecast_start=FORECAST_START)
    day = pd.Timestamp("2026-03-31")
    row = out[(out["target_date"] == day) & (out["country"] == "ALL")
              & (out["population"] == "ALL")]
    expected = 1_000_000.0 + measured_paid.loc[day].sum()
    assert row["DAU"].iloc[0] == pytest.approx(expected)


def test_non_fenix_populations_are_untouched_by_the_add_back(add_back_inputs):
    measured_paid, marketing, shares = add_back_inputs
    frame = _forecast_frame()
    out = add_paid_to_forecast(frame, measured_paid=measured_paid, marketing_paid=marketing,
                               country_shares=shares, forecast_start=FORECAST_START)
    ios = out["population"] == "firefox_ios"
    assert (out.loc[ios, "DAU"].to_numpy() == frame.loc[ios, "DAU"].to_numpy()).all()


def test_add_back_does_not_mutate_the_input(add_back_inputs):
    measured_paid, marketing, shares = add_back_inputs
    frame = _forecast_frame()
    before = frame["DAU"].copy()
    add_paid_to_forecast(frame, measured_paid=measured_paid, marketing_paid=marketing,
                         country_shares=shares, forecast_start=FORECAST_START)
    pd.testing.assert_series_equal(frame["DAU"], before)


def test_seam_step_reports_the_measured_vs_marketing_disagreement(add_back_inputs):
    measured_paid, marketing, _ = add_back_inputs
    step = paid_seam_step(measured_paid, marketing, training_end_date=TRAINING_END)
    assert step["step_abs"] == pytest.approx(
        step["marketing_paid_mean"] - step["measured_paid_mean"])
    assert step["marketing_first_day"] == pytest.approx(marketing.loc[FORECAST_START])
    assert step["measured_paid_mean"] > 0


# =============================================================================
# Spec loading
# =============================================================================

def test_load_organic_spec_rejects_the_wrong_type(organic_spec_dir):
    path = organic_spec_dir / "wrong.json"
    path.write_text(json.dumps({"type": "marketing_lift"}))
    with pytest.raises(ValueError, match="expected type 'paid_organic_split'"):
        load_organic_spec(path)


@pytest.mark.parametrize("missing", ["data_file", "share_column", "scope", "allocation"])
def test_load_organic_spec_names_the_missing_key(organic_spec_dir, missing):
    spec = json.loads((organic_spec_dir / "organic.json").read_text())
    del spec[missing]
    path = organic_spec_dir / "broken.json"
    path.write_text(json.dumps(spec))
    with pytest.raises(ValueError, match=f"missing required key '{missing}'"):
        load_organic_spec(path)


def test_load_organic_spec_rejects_an_unknown_allocation_key(organic_spec_dir):
    spec = json.loads((organic_spec_dir / "organic.json").read_text())
    spec["allocation"]["key"] = "uniform"
    path = organic_spec_dir / "broken.json"
    path.write_text(json.dumps(spec))
    with pytest.raises(ValueError, match="allocation.key must be one of"):
        load_organic_spec(path)


def test_load_organic_spec_rejects_an_unimplemented_backfill_policy(organic_spec_dir):
    spec = json.loads((organic_spec_dir / "organic.json").read_text())
    spec["share_backfill"]["policy"] = "extrapolate_trend"
    path = organic_spec_dir / "broken.json"
    path.write_text(json.dumps(spec))
    with pytest.raises(ValueError, match="is not implemented"):
        load_organic_spec(path)


def test_load_split_frame_rejects_duplicate_country_days(organic_spec_dir, split):
    doubled = pd.concat([split, split.head(1)], ignore_index=True)
    doubled.to_parquet(organic_spec_dir / "dupes.parquet", index=False)
    spec = load_organic_spec(organic_spec_dir / "organic.json")
    spec["data_file"] = "dupes.parquet"
    with pytest.raises(ValueError, match="duplicate"):
        load_split_frame(spec, organic_spec_dir)


# =============================================================================
# The real on-disk August artifacts
# =============================================================================

REAL_SPEC = REPO_ROOT / "data-official" / "2026-08" / "organic" / "organic.json"


@pytest.mark.skipif(not REAL_SPEC.exists(), reason="August organic spec not present")
def test_real_august_spec_loads_and_its_data_file_exists():
    """The `o` overlay has this test and `m` never did; a broken spec should fail here,
    not 20 minutes into a forecast run."""
    spec = load_organic_spec(REAL_SPEC)
    split = load_split_frame(spec, REAL_SPEC.parent)
    # Cycle-scoped pins: both moved with the 2026-08-03 data refresh (seam 2026-07-28 -> 2026-08-02,
    # training end 2026-07-27 -> 2026-08-01). Repoint them on every refresh, and keep the invariant
    # below them -- the split must cover training exactly through the day before the seam, or the
    # applier silently falls back to a held-flat share for the uncovered tail.
    assert spec["applies_to_forecast_start"] == "2026-08-02"
    assert spec["scope"]["exclude_countries"] == ["IR"]
    assert split["submission_date"].max() == pd.Timestamp("2026-08-01")
    assert split["submission_date"].max() == pd.Timestamp(
        spec["applies_to_forecast_start"]) - pd.Timedelta(days=1), (
        "the split must cover training through the day before the seam"
    )
    assert split["organic_share"].between(0, 1, inclusive="right").all()


@pytest.mark.skipif(not REAL_SPEC.exists(), reason="August organic spec not present")
def test_real_august_marketing_level_matches_the_published_anchor_and_lift():
    """Pins the Dec-15 paid level the August build is built on: anchor 922,250.47 + lift
    637,226.74 = 1,559,477. A silent re-vendor of the marketing curve trips this."""
    spec = load_organic_spec(REAL_SPEC)
    level = marketing_paid_level(spec, REAL_SPEC.parent,
                                 forecast_start=spec["applies_to_forecast_start"],
                                 forecast_end="2027-12-31")
    assert level.loc[pd.Timestamp("2026-12-15")] == pytest.approx(1_559_477.2, abs=1.0)
    # And the 2027 tail holds rather than collapsing.
    assert level.loc[pd.Timestamp("2027-06-01")] == pytest.approx(
        level.loc[pd.Timestamp("2026-12-31")])


# =============================================================================
# The output contract `p` must not break
# =============================================================================

def test_mobile_app_name_set_is_unchanged_by_the_split():
    """`p` decomposes DAU but must NOT add an organic/paid dimension to the output.

    The BQ mart has a single `dau` column and validation fixes the mobile app_name set, so a
    future attempt to emit `organic`/`paid` pseudo-apps has to be a deliberate schema change,
    not something that slips through. Published rows stay TOTAL DAU; the decomposition lives
    in the notebook and the docs.
    """
    from mozaic_daily.queries import DataSource
    from mozaic_daily.validation import _APP_NAMES_BY_DATA_SOURCE

    assert _APP_NAMES_BY_DATA_SOURCE[DataSource.GLEAN_MOBILE] == {
        "fenix_android", "firefox_ios", "focus_android", "focus_ios", "ALL MOBILE",
    }


def test_september_gmio_paid_curve_is_a_lift_whose_anchor_recovers_the_level():
    """data-official/2026-09/marketing (2026-09-04): the parquet is a lift in August's framing, the meta
    carries the anchor, and lift + anchor reproduces the level column. organic.json must copy that anchor."""
    import json
    marketing = REAL_SPEC.parent.parent.parent / "2026-09" / "marketing"
    df = pd.read_parquet(marketing / "marketing_lift_model.gmio_uac_meta_total.2026-09-02.parquet")
    meta = json.loads((marketing / "marketing_lift_model.gmio_uac_meta_total.2026-09-02.meta.json").read_text())
    anchor = meta["key_values"]["anchor_paid_dau"]
    assert df.loc["2026-03-30", "marketing_lift_daily"] == 0.0
    assert (df.loc[:"2026-03-29", "marketing_lift_daily"] == 0.0).all()
    recovered = df["marketing_lift_daily"].loc["2026-03-30":] + anchor
    pd.testing.assert_series_equal(recovered, df["paid_dau_level_daily"].loc["2026-03-30":], check_names=False)
    assert df.loc["2026-12-15", "paid_dau_level_daily"] == pytest.approx(1891001.857142857, abs=1.0)
    assert df.loc["2026-12-31", "paid_dau_level_daily"] == df.loc["2026-12-21", "paid_dau_level_daily"]  # forward-filled tail
