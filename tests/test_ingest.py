"""Tests for the ingest step (``ingest_inspect`` + ``ingest_build``).

What would ship a wrong adjustment: a misidentified column, a weekly file treated as daily,
a curve that starts after the seam or stops before year end, a moving average read as a
daily series, a sign the user did not confirm, a horizon tail that cliffs to zero, a
registry collision, or a parquet the pipeline cannot load. Every test builds its input in
``tmp_path``; the end-to-end test then loads the result through the real ``resolve_overlays``
and ``load_lift_series`` so the artifact contract is exercised, not restated.
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from mozaic_daily.adjustments import load_lift_series, render_adjustment
from mozaic_daily.ingest_build import (
    IngestPlan,
    build,
    build_horizon_curve,
    ensure_gitignore_exceptions,
    normalize_curve,
    registry_entry_text,
    registry_status,
)
from mozaic_daily.ingest_inspect import (
    contract_findings,
    detect_cadence,
    guess_value_columns,
    inspect_file,
    read_source_table,
)
from mozaic_daily.overlays import resolve_overlays

SEAM = "2026-09-02"
YEAR_END = pd.Timestamp("2026-12-31")
HORIZON_END = pd.Timestamp("2027-12-31")


def _daily(start="2026-06-01", end="2026-12-31", value=1000.0, actuals_through="2026-08-30", noise=True):
    idx = pd.date_range(start, end, freq="D")
    rng = np.random.default_rng(0)
    values = np.full(len(idx), float(value)) + (rng.normal(0, value * 0.1, len(idx)) if noise else 0.0)
    types = np.where(idx <= pd.Timestamp(actuals_through), "actuals", "forecast")
    return pd.DataFrame({"submission_date": idx.strftime("%Y-%m-%d"), "type": types, "dau": values.round()})


def _write_csv(tmp_path, frame, name="curve.csv"):
    path = tmp_path / name
    frame.to_csv(path, index=False)
    return path


def _repo(tmp_path) -> Path:
    root = tmp_path / "repo"
    (root / "data-official" / "2026-09").mkdir(parents=True)
    (root / "data-official" / "adjustment_codes.yaml").write_text(
        "# header comment must survive\ncodes:\n  l:\n    name: launch_on_login\n    applier: per_tile_overlay\n"
        "    description: >\n      existing\n    spec_glob: \"data-official/*/launch_on_login/lol.json\"\n"
    )
    (root / ".gitignore").write_text("*.parquet\n*.csv\n")
    return root


# --- reading + guessing ------------------------------------------------------------

class TestReadAndGuess:
    def test_reads_csv_with_thousands_separators_and_excel(self, tmp_path):
        frame = _daily()
        frame["dau"] = frame["dau"].map(lambda v: f"{int(v):,}")
        csv_frame, _ = read_source_table(_write_csv(tmp_path, frame))
        xlsx = tmp_path / "curve.xlsx"
        _daily().to_excel(xlsx, index=False, sheet_name="curve")
        xlsx_frame, sheet = read_source_table(xlsx)
        assert len(csv_frame) == len(xlsx_frame) == len(frame)
        assert sheet == "curve"

    def test_guesses_the_three_contract_columns(self, tmp_path):
        report = inspect_file(_write_csv(tmp_path, _daily()), forecast_start=SEAM)
        assert (report.date_column.column, report.value_column.column, report.type_column.column) == (
            "submission_date", "dau", "type")
        assert report.cadence == "daily" and report.sign_guess == "tailwind"
        assert report.actuals_through == "2026-08-30"
        assert not report.halts

    def test_ma_twin_is_recognised_by_name_and_not_chosen_as_the_value(self):
        frame = _daily()
        frame["dau_28ma"] = frame["dau"].rolling(28, min_periods=1).mean()
        value, ma = guess_value_columns(frame, exclude={"submission_date", "type"})
        assert value.column == "dau" and ma.column == "dau_28ma"

    def test_ma_twin_is_recognised_by_smoothness_when_names_do_not_help(self):
        frame = _daily()
        frame["smoothed"] = frame["dau"].rolling(28, min_periods=1).mean()
        frame = frame.rename(columns={"dau": "value"})
        value, ma = guess_value_columns(frame, exclude={"submission_date", "type"})
        assert value.column == "value" and ma.column == "smoothed"

    def test_mau_column_is_not_mistaken_for_a_moving_average(self):
        """Seen on the japan_bot handoff: 'ma' inside 'mau' matched the old substring hint."""
        frame = _daily().rename(columns={"dau": "japan_bot_dau_daily"})
        frame["japan_bot_dau_ma"] = frame["japan_bot_dau_daily"].rolling(28, min_periods=1).mean()
        frame["japan_bot_mau_daily"] = frame["japan_bot_dau_daily"] * 4
        frame["japan_bot_mau_ma"] = frame["japan_bot_mau_daily"].rolling(28, min_periods=1).mean()
        value, ma = guess_value_columns(frame, exclude={"submission_date", "type"})
        assert value.column == "japan_bot_dau_daily"
        assert ma.column == "japan_bot_dau_ma"

    def test_type_labels_are_normalised_and_odd_labels_rejected(self, tmp_path):
        frame = _daily()
        frame["type"] = frame["type"].map({"actuals": "Measured", "forecast": "Projected"})
        assert not inspect_file(_write_csv(tmp_path, frame), forecast_start=SEAM).halts
        frame.loc[5, "type"] = "guess"
        report = inspect_file(_write_csv(tmp_path, frame), forecast_start=SEAM)
        assert report.type_column.column is None  # 'guess' is not a known label, so no flag column is recognised
        assert any(f.code == "no_type_column" for f in report.findings)


# --- contract checks -----------------------------------------------------------------

class TestContract:
    def test_weekly_rows_halt(self, tmp_path):
        idx = pd.date_range("2026-06-01", "2027-01-04", freq="7D")
        frame = pd.DataFrame({"submission_date": idx, "dau": 7000.0})
        report = inspect_file(_write_csv(tmp_path, frame), forecast_start=SEAM)
        assert report.cadence == "weekly" and report.halts
        assert any(f.code == "weekly_rows" for f in report.findings)
        assert detect_cadence(pd.DatetimeIndex(idx)) == "weekly"

    def test_start_after_seam_and_end_before_year_end_are_errors(self):
        late_start = pd.date_range("2026-09-10", "2026-12-31", freq="D")
        findings = contract_findings(late_start, pd.Series(1.0, index=range(len(late_start))), None,
                                     forecast_start=pd.Timestamp(SEAM), forecast_year_end=YEAR_END, horizon_end=HORIZON_END)
        assert {f.code for f in findings if f.level == "error"} == {"starts_after_seam"}
        short = pd.date_range("2026-06-01", "2026-11-30", freq="D")
        findings = contract_findings(short, pd.Series(1.0, index=range(len(short))), None,
                                     forecast_start=pd.Timestamp(SEAM), forecast_year_end=YEAR_END, horizon_end=HORIZON_END)
        assert {f.code for f in findings if f.level == "error"} == {"ends_before_year_end"}

    def test_full_horizon_file_needs_no_hold_and_short_one_is_flagged_info(self):
        full = pd.date_range("2026-06-01", "2027-12-31", freq="D")
        codes = {f.code for f in contract_findings(full, pd.Series(1.0, index=range(len(full))), None,
                                                    forecast_start=pd.Timestamp(SEAM), forecast_year_end=YEAR_END,
                                                    horizon_end=HORIZON_END)}
        assert "hold_flat_tail" not in codes
        to_year_end = pd.date_range("2026-06-01", "2026-12-31", freq="D")
        codes = {f.code for f in contract_findings(to_year_end, pd.Series(1.0, index=range(len(to_year_end))), None,
                                                    forecast_start=pd.Timestamp(SEAM), forecast_year_end=YEAR_END,
                                                    horizon_end=HORIZON_END)}
        assert "hold_flat_tail" in codes

    def test_file_boundary_offset_from_seam_is_informational(self, tmp_path):
        """The producer's actuals may end before the forecast seam; that is allowed, only coverage matters."""
        report = inspect_file(_write_csv(tmp_path, _daily(actuals_through="2026-08-30")), forecast_start=SEAM)
        offset = [f for f in report.findings if f.code == "file_boundary_before_seam"]
        assert len(offset) == 1 and offset[0].level == "info" and "2 day" in offset[0].message
        assert not report.halts

    def test_actuals_after_forecast_is_an_error(self):
        idx = pd.date_range("2026-06-01", "2026-12-31", freq="D")
        types = pd.Series(np.where(idx <= "2026-08-30", "actuals", "forecast"))
        types.iloc[-1] = "actuals"
        findings = contract_findings(idx, pd.Series(1.0, index=range(len(idx))), types,
                                     forecast_start=pd.Timestamp(SEAM), forecast_year_end=YEAR_END, horizon_end=HORIZON_END)
        assert any(f.code == "actuals_after_forecast" for f in findings)

    def test_moving_average_and_cumulative_series_warn(self, tmp_path):
        frame = _daily()
        frame["dau"] = frame["dau"].rolling(28, min_periods=1).mean()
        report = inspect_file(_write_csv(tmp_path, frame), forecast_start=SEAM)
        assert any(f.code == "looks_like_moving_average" for f in report.findings)
        cumulative = _daily(noise=False)
        cumulative["dau"] = cumulative["dau"].cumsum()
        report = inspect_file(_write_csv(tmp_path, cumulative), forecast_start=SEAM)
        assert any(f.code == "monotone_increasing" for f in report.findings)

    def test_flat_tails_do_not_make_a_daily_measured_block_look_smoothed(self, tmp_path):
        """Seen on the japan_bot handoff: 84 zero days + 365 held-flat days dominated the smoothness ratio."""
        frame = _daily(start="2026-04-01", end="2027-12-31", actuals_through="2026-08-30")
        dates = pd.to_datetime(frame["submission_date"])
        frame.loc[dates < "2026-06-24", "dau"] = 0.0
        frame.loc[dates > "2026-08-30", "dau"] = 67101.0
        report = inspect_file(_write_csv(tmp_path, frame), forecast_start=SEAM)
        assert not any(f.code == "looks_like_moving_average" for f in report.findings)

    def test_mixed_sign_warns_and_negative_file_guesses_headwind(self, tmp_path):
        frame = _daily()
        frame["dau"] = -frame["dau"]
        assert inspect_file(_write_csv(tmp_path, frame), forecast_start=SEAM).sign_guess == "headwind"
        frame.loc[frame.index % 2 == 0, "dau"] *= -1
        report = inspect_file(_write_csv(tmp_path, frame), forecast_start=SEAM)
        assert report.sign_guess == "mixed" and any(f.code == "mixed_sign" for f in report.findings)


# --- horizon ------------------------------------------------------------------------------

def _plan(root, **overrides) -> IngestPlan:
    base = dict(source_path=str(root / "curve.csv"), name="test_wind", code="q", family="per_tile_overlay",
                platform="desktop", data_source="legacy_desktop", forecast_start=SEAM, cycle="2026-09",
                date_column="submission_date", value_column="dau", type_column="type", root=str(root),
                description="A test tailwind.")
    base.update(overrides)
    return IngestPlan(**base)


class TestHorizon:
    def test_zero_before_verbatim_inside_flat_after_at_final_28d_mean(self, tmp_path):
        root = _repo(tmp_path)
        frame = _daily(noise=False, value=1000.0)
        frame.loc[frame.index[-28:], "dau"] = 2000.0  # last 28 delivered days are 2000
        curve = normalize_curve(frame, _plan(root))
        horizon, summary = build_horizon_curve(curve, _plan(root))
        daily = horizon["test_wind_dau_daily"]
        assert daily[pd.Timestamp("2026-01-01")] == 0.0 and daily[pd.Timestamp("2026-05-31")] == 0.0
        assert daily[pd.Timestamp("2026-08-01")] == 1000.0
        assert daily[pd.Timestamp("2027-01-01")] == 2000.0 and daily[pd.Timestamp("2027-12-31")] == 2000.0
        assert summary["hold_flat_from"] == "2027-01-01" and summary["hold_flat_value"] == 2000.0
        assert set(horizon["source"].unique()) == {"pre-onset", "measured", "projected", "held"}
        assert horizon.index.name == "target_date" and isinstance(horizon.index, pd.DatetimeIndex)

    def test_sign_flip_and_interpolated_gap(self, tmp_path):
        root = _repo(tmp_path)
        frame = _daily(noise=False, value=100.0).drop(index=[10, 11])  # two skipped days
        curve = normalize_curve(frame, _plan(root, sign=-1))
        assert curve["dau"].iloc[10] == -100.0 and len(curve) == len(pd.date_range("2026-06-01", "2026-12-31"))
        horizon, summary = build_horizon_curve(curve, _plan(root, sign=-1))
        assert summary["dec15_ma28"] == -100.0

    def test_rebase_to_seam_zeroes_the_seam_and_keeps_the_delivered_series(self, tmp_path):
        """Brad's Win10 curve ramps from the August seam; September applies only the increment from its own seam."""
        root = _repo(tmp_path)
        idx = pd.date_range("2026-08-02", "2026-12-31", freq="D")
        ramp = np.maximum(-726000.0 * np.asarray((idx - idx[0]).days) / (pd.Timestamp("2026-12-15") - idx[0]).days, -726000.0)
        frame = pd.DataFrame({"submission_date": idx.strftime("%Y-%m-%d"), "dau_28ma": ramp.round()})
        plan = _plan(root, family="display_layer", type_column=None, actuals_through="2026-08-01",
                     value_column="dau_28ma", values_are_28d_ma=True, rebase_to_seam=True)
        horizon, summary = build_horizon_curve(normalize_curve(frame, plan), plan)
        daily = horizon["test_wind_dau_daily"]
        assert daily[pd.Timestamp("2026-09-01")] == 0.0 and daily[pd.Timestamp(SEAM)] == 0.0
        assert summary["rebase_offset_at_seam"] == pytest.approx(-166711.0, abs=1)
        assert daily[pd.Timestamp("2026-12-15")] == pytest.approx(-726000.0 + 166711.0, abs=1)
        assert daily[pd.Timestamp("2027-12-31")] == daily[pd.Timestamp("2026-12-15")]  # held at the final value
        assert horizon["test_wind_dau_delivered"][pd.Timestamp("2026-12-15")] == pytest.approx(-726000.0, abs=1)
        assert (horizon["test_wind_dau_ma"] == daily).all()  # already a 28-day series: not re-smoothed

    def test_actuals_through_replaces_a_missing_type_column(self, tmp_path):
        root = _repo(tmp_path)
        frame = _daily(noise=False).drop(columns="type")
        curve = normalize_curve(frame, _plan(root, type_column=None, actuals_through="2026-07-15"))
        assert curve.loc[pd.Timestamp("2026-07-15"), "type"] == "actuals"
        assert curve.loc[pd.Timestamp("2026-07-16"), "type"] == "forecast"


# --- plan validation + registry --------------------------------------------------------------

class TestPlanAndRegistry:
    def test_plan_rejects_bad_inputs(self, tmp_path):
        root = _repo(tmp_path)
        with pytest.raises(ValueError, match="single lowercase"):
            _plan(root, code="jb")
        with pytest.raises(ValueError, match="shares dict"):
            _plan(root, allocation="fixed_country_shares")
        with pytest.raises(ValueError, match="actuals_through"):
            _plan(root, type_column=None)

    def test_update_of_a_registered_code_follows_its_spec_glob_layout(self, tmp_path):
        """`o` is registered as mozillaonline_migration but lives in mozillaonline/mozillaonline.json;
        a build keyed on the registry name would land where the dispatcher never looks."""
        root = _repo(tmp_path)
        registry = root / "data-official" / "adjustment_codes.yaml"
        registry.write_text(registry.read_text() + (
            "  o:\n    name: mozillaonline_migration\n    applier: per_tile_overlay\n"
            "    description: >\n      x\n    spec_glob: \"data-official/*/mozillaonline/mozillaonline.json\"\n"
            "  l:\n    name: launch_at_login_new_users\n    applier: per_tile_overlay\n    description: >\n      x\n"
            "    spec_glob:\n      - \"data-official/*/launch_at_login_new_users/launch_at_login_new_users.json\"\n"
            "      - \"data-official/*/launch_on_login/lol.json\"\n"))
        listed = _plan(root, code="l", name="launch_at_login_new_users")
        assert listed.curve_dir.name == "launch_at_login_new_users"  # first glob wins for updates
        plan = _plan(root, code="o", name="mozillaonline_migration")
        assert plan.curve_dir == root / "data-official" / "2026-09" / "mozillaonline"
        assert plan.spec_path.name == "mozillaonline.json"
        assert plan.spec_glob == "data-official/*/mozillaonline/mozillaonline.json"
        fresh = _plan(root)  # unregistered code: <name>/<name>.json
        assert fresh.curve_dir.name == "test_wind" and fresh.spec_path.name == "test_wind.json"

    def test_registry_status_and_collisions(self, tmp_path):
        root = _repo(tmp_path)
        assert registry_status(_plan(root)) == "new"
        assert registry_status(_plan(root, code="l", name="launch_on_login")) == "same"
        with pytest.raises(ValueError, match="already registered as 'launch_on_login'"):
            registry_status(_plan(root, code="l"))
        with pytest.raises(ValueError, match="already registered under code 'l'"):
            registry_status(_plan(root, code="z", name="launch_on_login"))

    def test_registry_entry_text_parses_and_gitignore_is_idempotent(self, tmp_path):
        root = _repo(tmp_path)
        plan = _plan(root, description="Line one.\nLine two.")
        parsed = yaml.safe_load("codes:\n" + registry_entry_text(plan))
        assert parsed["codes"]["q"]["applier"] == "per_tile_overlay"
        assert parsed["codes"]["q"]["spec_glob"] == "data-official/*/test_wind/test_wind.json"
        assert ensure_gitignore_exceptions(plan) == ["!data-official/*/test_wind/*.parquet", "!data-official/*/test_wind/source_data/*"]
        assert ensure_gitignore_exceptions(plan) == []


# --- end to end -------------------------------------------------------------------------------

class TestBuildEndToEnd:
    def test_overlay_build_resolves_through_the_registry_and_loads(self, tmp_path):
        root = _repo(tmp_path)
        frame = _daily(noise=False, value=500.0)
        frame.to_csv(root / "curve.csv", index=False)
        plan = _plan(root, allocation="fixed_country_shares", shares={"JP": 1.0}, exclude_countries=[])
        summary = build(plan, frame)

        assert summary["registry"] == "new"
        assert "# header comment must survive" in (root / "data-official" / "adjustment_codes.yaml").read_text()
        registry = yaml.safe_load((root / "data-official" / "adjustment_codes.yaml").read_text())["codes"]
        resolved = resolve_overlays(SEAM, root=root, registry=registry)
        assert [o.code for o in resolved] == ["q"]
        overlay = resolved[0]
        series = load_lift_series(overlay.spec, overlay.spec_path.parent)
        assert series[pd.Timestamp("2026-12-15")] == 500.0 and series[pd.Timestamp("2027-12-31")] == 500.0
        assert overlay.spec["allocation"]["shares"] == {"JP": 1.0}

        files = {Path(p).name for p in summary["files"].values()}
        assert {"test_wind.json", "test_wind.2026-08-30.parquet", "test_wind.2026-08-30.csv",
                "test_wind.2026-08-30.meta.json", "curve.csv", "_index.md", "test_wind.2026-08-30.curve.png"} <= files
        assert Path(summary["files"]["plot"]).stat().st_size > 10_000  # a rendered PNG, not an empty file
        meta = json.loads(Path(summary["files"]["meta"]).read_text())
        assert meta["adjustment_code"] == "q" and meta["source_sha1"] and meta["coverage"]["hold_flat_from"] == "2027-01-01"
        assert (root / "data-official" / "2026-09" / "test_wind" / "source_data" / "curve.csv").read_bytes() == (root / "curve.csv").read_bytes()

    def test_display_layer_gitignore_exceptions_name_the_curve_dir_not_adjustments(self, tmp_path):
        root = _repo(tmp_path)
        plan = _plan(root, family="display_layer")
        assert ensure_gitignore_exceptions(plan) == ["!data-official/*/test_wind/*.parquet", "!data-official/*/test_wind/source_data/*"]

    def test_second_build_needs_replace_and_stashes_a_revert_dir(self, tmp_path):
        root = _repo(tmp_path)
        frame = _daily(noise=False, value=500.0)
        frame.to_csv(root / "curve.csv", index=False)
        build(_plan(root), frame)
        with pytest.raises(FileExistsError, match="--replace"):
            build(_plan(root), frame)
        frame["dau"] = 700.0
        summary = build(_plan(root, replace=True), frame)
        assert summary["registry"] == "same"
        revert = Path(summary["revert_dir"])
        assert (revert / "REVERT.md").exists() and (revert / "test_wind.json").exists()
        assert (revert / "test_wind.2026-08-30.parquet").exists()
        new_series = pd.read_parquet(summary["files"]["parquet"])["test_wind_dau_daily"]
        assert new_series[pd.Timestamp("2026-12-15")] == 700.0

    def test_display_layer_build_renders_through_the_package(self, tmp_path):
        root = _repo(tmp_path)
        frame = _daily(noise=False, value=280.0)
        frame.to_csv(root / "curve.csv", index=False)
        summary = build(_plan(root, family="display_layer", platform="mobile"), frame)
        spec_path = Path(summary["files"]["spec"])
        assert spec_path.parent.name == "adjustments"
        spec = json.loads(spec_path.read_text())
        idx = pd.date_range("2026-06-01", "2027-12-31", freq="D")
        rendered = render_adjustment(spec, idx, spec_dir=spec_path.parent)
        assert rendered["mobile"][pd.Timestamp("2026-12-15")] == pytest.approx(280.0)
        assert (rendered["desktop"] == 0).all()
