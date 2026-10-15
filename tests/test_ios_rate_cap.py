# -*- coding: utf-8 -*-
"""Tests for the iOS-rate cap (``mozaic_daily.ios_rate_cap``): the cap path arithmetic and its application to a training frame.

Synthetic series with hand-computed expectations, so a sign flip, a missed smoothing, a cap that adds instead of removes,
or a cap applied to the wrong rows all change a literal.
"""

import numpy as np
import pandas as pd
import pytest

from mozaic_daily.ios_rate_cap import CAP_COLUMNS, apply_ios_rate_cap, build_cap_frame, ios_rate_cap_path

DATES = pd.date_range("2026-01-01", periods=10, freq="D")
FROM = pd.Timestamp("2026-01-05")


def _series(values):
    return pd.Series(values, index=DATES, dtype=float)


def test_cap_path_is_exact_at_from_date_follows_the_smoothed_ios_ratio_and_only_removes():
    # organic jumps +50% after the from-date; iOS grows 10% per day in level from the from-date on
    organic = _series([100] * 5 + [150, 150, 150, 150, 150])
    ios = _series([200] * 5 + [220, 242, 266.2, 292.82, 322.1])
    path = ios_rate_cap_path(organic, ios, from_date=FROM, smoothing_days=2)
    assert list(path.index) == list(DATES[4:])
    assert path.loc[FROM, "removed_mau"] == 0 and path.loc[FROM, "cap_level"] == 100   # ratio 1 at the from-date
    # day after: iOS 2-day mean = (200+220)/2 = 210 vs 200 at the from-date -> cap 105, organic 150 -> 45 removed
    assert path.loc[DATES[5], "cap_level"] == pytest.approx(105)
    assert path.loc[DATES[5], "capped_mau"] == 105 and path.loc[DATES[5], "removed_mau"] == 45
    assert (path["removed_mau"] >= 0).all()
    # once the iOS path overtakes organic the cap stops binding: last day 2-day mean (292.82+322.1)/2 = 307.46 -> cap 153.73 > 150
    assert path.loc[DATES[-1], "removed_mau"] == 0 and path.loc[DATES[-1], "capped_mau"] == 150


def test_cap_path_never_adds_when_organic_falls_below_the_ios_path():
    organic = _series([100] * 5 + [80] * 5)
    ios = _series([200] * 10)
    path = ios_rate_cap_path(organic, ios, from_date=FROM)
    assert (path["removed_mau"] == 0).all() and (path["capped_mau"] == path["organic_mau"]).all()


def test_cap_path_requires_the_from_date_in_both_series():
    with pytest.raises(ValueError, match="from_date"):
        ios_rate_cap_path(_series([1.0] * 10), _series([1.0] * 10).iloc[:3], from_date=FROM)


def test_build_cap_frame_covers_only_the_named_countries_from_the_from_date():
    organic = pd.DataFrame({"IN": [100] * 10, "ID": [50] * 10, "US": [900] * 10}, index=DATES, dtype=float)
    organic.loc[DATES[6:], "IN"] = 130
    ios = pd.DataFrame({"IN": [10] * 10, "ID": [5] * 10, "US": [90] * 10}, index=DATES, dtype=float)
    cap = build_cap_frame(organic, ios, countries=["IN", "ID"], from_date=FROM)
    assert list(cap.columns) == CAP_COLUMNS
    assert set(cap["country"]) == {"IN", "ID"} and cap["submission_date"].min() == FROM
    assert cap[cap.country == "IN"]["removed_mau"].tolist() == [0, 0, 30, 30, 30, 30]
    assert (cap[cap.country == "ID"]["removed_mau"] == 0).all()
    with pytest.raises(ValueError, match="FR"):
        build_cap_frame(organic, ios, countries=["FR"], from_date=FROM)


def _training(in_values):
    rows = []
    for d, v in zip(DATES, in_values):
        for c, y_fenix in (("IN", v), ("US", 900)):
            rows.append({"x": d, "country": c, "fenix_android": True, "firefox_ios": False, "y": y_fenix})
            rows.append({"x": d, "country": c, "fenix_android": False, "firefox_ios": True, "y": 10})
    return pd.DataFrame(rows).astype({"y": "Int64"})


def test_apply_subtracts_removed_from_matching_fenix_rows_only_and_records():
    training = _training([100] * 6 + [130] * 4)
    cap = pd.DataFrame({"submission_date": DATES[4:], "country": "IN", "organic_mau": [100, 100, 130, 130, 130, 130],
                        "removed_mau": [0, 0, 30, 30, 30, 30]})
    out, record = apply_ios_rate_cap(training, cap)
    in_fenix = out[(out.country == "IN") & out.fenix_android].set_index("x")["y"].astype(int)
    assert in_fenix.tolist() == [100] * 10                       # the excess is gone, pre-from-date rows untouched
    assert (out[(out.country == "US") & out.fenix_android]["y"] == 900).all()
    assert (out[out.firefox_ios]["y"] == 10).all()               # iOS rows (the reference) are never touched
    assert (training[(training.country == "IN") & training.fenix_android]["y"].astype(int).tolist()[-1]) == 130   # input not mutated
    assert record["removed_mau_last_day"] == {"IN": 30} and record["removed_mau_last_day_total"] == 30
    assert record["rows_capped"] == 6 and record["from_date"] == "2026-01-05"
    assert record["peak_removed_share_of_organic"]["IN"] == pytest.approx(30 / 130)
    with pytest.raises(RuntimeError, match="twice"):
        apply_ios_rate_cap(out, cap)


def test_apply_refuses_a_cap_built_on_a_different_organic_series():
    training = _training([100] * 10)
    cap = pd.DataFrame({"submission_date": DATES[4:6], "country": "IN", "organic_mau": [100, 120], "removed_mau": [0, 20]})
    with pytest.raises(ValueError, match="differ from the organic series"):
        apply_ios_rate_cap(training, cap)


def test_apply_refuses_cap_rows_with_no_training_row():
    training = _training([100] * 10)
    cap = pd.DataFrame({"submission_date": [DATES[4]], "country": ["FR"], "organic_mau": [100], "removed_mau": [0]})
    with pytest.raises(ValueError, match="no matching"):
        apply_ios_rate_cap(training, cap)


def test_round2_grid_is_six_capped_probes_in_its_own_directory():
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
    import run_mobile_mau_probes as probes
    grid = probes.GRIDS["round2"]
    assert grid["ios_rate_cap"] is True and len(grid["knobs"]) == 6
    assert {k["changepoint_range"] for k in grid["knobs"]} == {0.80, 0.87}
    assert {k["changepoint_prior_scale"] for k in grid["knobs"]} == {0.005, 0.01, 0.02}
    assert probes.grid_dir("round2") != probes.grid_dir("round1")
    assert probes.GRIDS["round1"]["ios_rate_cap"] is False


def test_round3_grid_is_the_neighbourhood_of_the_round2_centre_without_repeating_it():
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
    import run_mobile_mau_probes as probes
    grid = probes.GRIDS["round3"]
    assert grid["ios_rate_cap"] is True and len(grid["knobs"]) == 11
    assert {"changepoint_range": 0.87, "changepoint_prior_scale": 0.005} not in grid["knobs"]   # reused from round 2
    assert {k["changepoint_range"] for k in grid["knobs"]} == {0.84, 0.87, 0.90}
    assert {k["changepoint_prior_scale"] for k in grid["knobs"]} == {0.004, 0.005, 0.0065, 0.008}
