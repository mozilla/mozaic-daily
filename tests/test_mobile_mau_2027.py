# -*- coding: utf-8 -*-
"""Tests for the 2027 mobile MAU companion build: the mobile_app raw-frame layout, the mobile fill plumbing, the runner's config.

Synthetic pulls only. Levels are chosen so a wrong flag assignment or a dropped row changes a sum checked against a literal.
"""

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import build_comparison_raw_frames as frames  # noqa: E402
import run_mobile_mau_forecast as runner  # noqa: E402

APPS = {"Fenix": 1000, "Firefox iOS": 200, "Focus Android": 30, "Focus iOS": 4}


def _mobile_pull(days=3, countries=("US", "ROW"), late_app=None):
    rows = []
    for d_i, d in enumerate(pd.date_range("2021-01-01", periods=days, freq="D")):
        for c_i, c in enumerate(countries):
            for app, level in APPS.items():
                if app == late_app and d_i == 0:
                    continue   # an app whose history starts a day later (Focus Android starts 2021-11-28 in the real pull)
                rows.append({"x": d.date().isoformat(), "country": c, "app_name": app,
                             "dau": level + c_i, "mau": 5 * level + c_i + d_i})
    return pd.DataFrame(rows).assign(x=lambda d: pd.to_datetime(d["x"]))   # load_pull parses x the same way


def test_mobile_layout_maps_each_app_to_exactly_one_production_flag():
    layout = frames.LAYOUTS["mobile_app"]
    out = frames.to_frames(_mobile_pull(), layout)
    mau = out["MAU"]
    assert list(mau.columns) == ["x", "country", "fenix_android", "firefox_ios", "focus_android", "focus_ios", "y"]
    assert (mau[list(layout.flags)].sum(axis=1) == 1).all()
    fenix_us_day0 = mau[(mau.country == "US") & mau.fenix_android & (mau.x == "2021-01-01")]["y"].item()
    assert fenix_us_day0 == 5000
    assert mau["y"].dtype == "int64"
    assert out["DAU"]["y"].sum() == 2 * 3 * sum(APPS.values()) + 3 * 4   # ROW rows carry +1


def test_mobile_layout_keeps_a_late_starting_app_but_desktop_layout_stays_strict():
    out = frames.to_frames(_mobile_pull(late_app="Focus Android"), frames.LAYOUTS["mobile_app"])
    focus = out["MAU"][out["MAU"].focus_android]
    assert focus["x"].min() == pd.Timestamp("2021-01-02") and len(focus) == 4
    desktop = pd.DataFrame({"x": pd.to_datetime(["2025-01-01", "2025-01-02", "2025-01-01"]), "country": ["US"] * 3,
                            "os_bucket": ["windows", "windows", "mac"], "dau": [1, 1, 1], "mau": [1, 1, 1]})
    with pytest.raises(ValueError, match="not dense"):
        frames.to_frames(desktop, frames.LAYOUTS["desktop_os"])


def test_unmapped_segment_values_are_dropped_not_folded():
    pull = _mobile_pull()
    pull.loc[len(pull)] = {"x": pd.Timestamp("2021-01-01"), "country": "US", "app_name": "Klar", "dau": 999999, "mau": 999999}
    out = frames.to_frames(pull, frames.LAYOUTS["mobile_app"])
    assert out["DAU"]["y"].max() == 1001


def test_mobile_forecast_wrapper_forwards_additional_fills(mocker):
    """The packaged mobile fill has no MAU metric; the runner's fill must reach populate_tiles."""
    from mozaic_daily import forecast as fc
    mocker.patch.object(fc.mozaic, "TileSet")
    populate = mocker.patch.object(fc.mozaic, "populate_tiles")
    mocker.patch.object(fc.mozaic.utils, "curate_mozaics")
    mocker.patch.object(fc, "_check_data_health")
    fill = pd.DataFrame({"metric": ["MAU"], "x": [pd.Timestamp("2026-03-01")], "country": ["IR"], "y": [1.0]})
    fc.get_mobile_forecast_dfs({"MAU": pd.DataFrame({"x": [], "country": [], "y": []})}, "2026-10-10", "2027-12-31",
                               data_source="glean_mobile", additional_fills=[fill])
    assert populate.call_args.kwargs["additional_fills"] == [fill]


def test_runner_default_config_is_the_october_mobile_lock_including_unexposed_radii():
    parser = runner.argparse.ArgumentParser()
    runner.add_knob_arguments(parser, defaults=runner.OCTOBER_MOBILE_LOCK)
    cfg = runner.build_config(parser.parse_args([]))
    assert cfg.to_dict() == runner.OCTOBER_MOBILE_LOCK
    assert cfg.to_slug() == "cps0.035_thresh055_recent13_cpr0.725_ncp25_clip0.6_sps0.1"
    october = Path(__file__).resolve().parent.parent / "data-official/2026-10/mobile_raw_2026-10-05"
    recorded = json.loads(next(october.glob("*/parameters.json")).read_text())["config"]
    assert recorded == runner.OCTOBER_MOBILE_LOCK


def test_runner_exposes_no_holiday_radius_knobs():
    assert not {"holiday_max_radius", "holiday_min_radius"} & {f for f, _ in runner.KNOBS.values()}


# ---- organic MAU split -------------------------------------------------------------------------

def test_mau_split_frame_partitions_and_renames():
    import build_fenix_organic_mau_split as bld
    pull = pd.DataFrame({
        "submission_date": pd.to_datetime(["2026-01-01"] * 3 + ["2026-01-01"]),
        "country": ["US", "US", "US", "DE"],
        "growth_source": ["organic", "paid_rolling_12mo", "paid_prior_1yr", "organic"],
        "mau": [900, 60, 40, 500],
    })
    split = bld.mau_split_frame(pull)
    assert list(split.columns) == ["submission_date", "country", "organic_mau", "paid_mau", "total_mau", "organic_share"]
    us = split[split.country == "US"].iloc[0]
    assert (us.organic_mau, us.paid_mau, us.total_mau, us.organic_share) == (900, 100, 1000, 0.9)
    assert split[split.country == "DE"].iloc[0].organic_share == 1.0


def test_runner_organic_split_scales_fenix_only_holds_share_backwards_and_skips_ir(tmp_path):
    dates = pd.date_range("2026-01-01", periods=4, freq="D")
    rows = []
    for d in dates:
        for c in ("US", "IR"):
            rows.append({"x": d, "country": c, "fenix_android": True, "firefox_ios": False, "focus_android": False, "focus_ios": False, "y": 1000})
            rows.append({"x": d, "country": c, "fenix_android": False, "firefox_ios": True, "focus_android": False, "focus_ios": False, "y": 300})
    frame = pd.DataFrame(rows)
    # split measured only from the third day: the first two days must hold the earliest share (0.8) flat backwards
    split = pd.DataFrame({"submission_date": list(dates[2:]) * 2, "country": ["US"] * 2 + ["IR"] * 2, "organic_share": [0.8, 0.5, 0.9, 0.9]})
    split_path = tmp_path / "split.parquet"
    split.to_parquet(split_path, index=False)
    organic, record = runner.apply_organic_split(frame, split_path, ["IR"])
    us_fenix = organic[(organic.country == "US") & organic.fenix_android].set_index("x")["y"].astype(int).tolist()
    assert us_fenix == [800, 800, 800, 500]
    assert (organic[(organic.country == "IR") & organic.fenix_android]["y"] == 1000).all()
    assert (organic[organic.firefox_ios]["y"] == 300).all()
    assert record["fenix_paid_mau_removed_last_day"] == 500 and record["fenix_world_total_mau_last_day"] == 2000
    assert record["share_measured_from"] == "2026-01-03"
