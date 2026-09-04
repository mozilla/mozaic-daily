"""Tests for scripts/export_desktop_no_headwind_csv.py.

The script publishes a counterfactual that looks superficially like the canonical forecast, so the
failure modes worth testing are the ones that would ship a *wrong* or *mislabelled* number:

- the ramp being reconstructed with different semantics than the notebook that produced the
  published file (clamping past the anchor, or not starting at zero at `start_date`);
- the ramp being applied before the seam, which would move history;
- the script stripping something that is not the Win10 headwind and still labelling it as one
  (other desktop-moving specs must be left in and reported, not folded in);
- the summary's ledger columns not actually recovering the published figures.

All of these are exercised against hand-built specs and series — no parquets, no BigQuery, and no
dependence on the real August CSVs (which change whenever a spec is edited).
"""

import importlib.util
import json
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "export_desktop_no_headwind_csv",
    REPO_ROOT / "scripts" / "export_desktop_no_headwind_csv.py",
)
export = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(export)

ANCHOR = pd.Timestamp("2026-12-15")
RAMP_START = pd.Timestamp("2026-08-02")
HEADWIND_SPEC = {
    "type": "linear_ramp",
    "start_date": "2026-08-02",
    "anchor_date": "2026-12-15",
    "desktop_dau": -1315000,
    "mobile_dau": -27162,
}


def _index():
    return pd.date_range("2026-01-01", "2026-12-31", freq="D")


def _write_specs(directory: Path, specs: dict[str, dict]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    for filename, spec in specs.items():
        (directory / filename).write_text(json.dumps(spec))


class TestRenderDesktopLinearRamp:
    """The ramp must match the notebook's `render_adjustment`, including its unclamped tail."""

    def test_zero_at_start_and_full_anchor_value_at_anchor(self):
        ramp = export.render_desktop_linear_ramp(HEADWIND_SPEC, _index())
        assert ramp[RAMP_START] == 0.0
        assert ramp[ANCHOR] == pytest.approx(-1315000.0)

    def test_one_day_step_is_anchor_over_ramp_length(self):
        ramp = export.render_desktop_linear_ramp(HEADWIND_SPEC, _index())
        ramp_days = (ANCHOR - RAMP_START).days
        assert ramp_days == 135
        assert ramp[RAMP_START + pd.Timedelta(days=1)] == pytest.approx(-1315000.0 / 135)

    def test_flat_zero_before_start_date(self):
        """`elapsed` is floored at 0, so a pre-start date must not get a positive ramp."""
        ramp = export.render_desktop_linear_ramp(HEADWIND_SPEC, _index())
        assert (ramp[ramp.index < RAMP_START] == 0.0).all()

    def test_overshoots_past_anchor_and_is_not_clamped(self):
        """The published file is unclamped past the anchor; clamping here would break the reversal.

        Expected value is 1,315,000 x 151/135 (2026-08-02 -> 12-31 over the 135-day ramp). See
        data-official/2026-08/adjustments/_index.md on the five diverging implementations — that
        table printed 156/135 until 2026-08-05, pairing the *previous* 2026-07-28 ramp's day count
        with the current ramp's length.
        """
        ramp = export.render_desktop_linear_ramp(HEADWIND_SPEC, _index())
        assert ramp[pd.Timestamp("2026-12-31")] == pytest.approx(-1470851.85, abs=0.01)

    def test_ignores_the_mobile_leg(self):
        """A desktop-only export must not pick up mobile_dau, which has the opposite sign story."""
        mobile_only = {**HEADWIND_SPEC, "desktop_dau": 0}
        ramp = export.render_desktop_linear_ramp(mobile_only, _index())
        assert (ramp == 0.0).all()


class TestLoadDesktopHeadwindRamp:
    """The loader's guards are the difference between a correct file and a mislabelled one."""

    def test_zeroes_the_ramp_before_the_seam(self, tmp_path):
        """History is actuals. A ramp leaking before the seam would rewrite observed data."""
        _write_specs(tmp_path, {"headwind.json": HEADWIND_SPEC})
        seam = pd.Timestamp("2026-09-01")
        ramp = export.load_desktop_headwind_ramp(str(tmp_path), _index(), seam)
        assert (ramp[ramp.index < seam] == 0.0).all()
        assert ramp[seam] < 0.0

    def test_ignores_a_spec_whose_desktop_leg_is_zero(self, tmp_path):
        """August's real `tailwind.json` is mobile-only; it must not affect the desktop output."""
        _write_specs(tmp_path, {
            "headwind.json": HEADWIND_SPEC,
            "tailwind.json": {**HEADWIND_SPEC, "desktop_dau": 0, "mobile_dau": 299000},
        })
        with_tailwind = export.load_desktop_headwind_ramp(str(tmp_path), _index(), RAMP_START)
        assert with_tailwind[ANCHOR] == pytest.approx(-1315000.0)

    def test_strips_only_headwind_json_and_reports_other_desktop_specs(self, tmp_path, capsys):
        """A second desktop-moving spec (e.g. a display-layer tailwind curve) must be left in
        the output and named on stdout, never silently folded into 'the Win10 headwind'."""
        _write_specs(tmp_path, {
            "headwind.json": HEADWIND_SPEC,
            "other.json": {**HEADWIND_SPEC, "desktop_dau": -50000},
        })
        ramp = export.load_desktop_headwind_ramp(str(tmp_path), _index(), RAMP_START)
        assert ramp[ANCHOR] == pytest.approx(-1315000.0)
        out = capsys.readouterr().out
        assert "other.json" in out and "LEFT IN" in out and "-50,000" in out

    def test_reports_nothing_when_other_specs_do_not_move_desktop(self, tmp_path, capsys):
        _write_specs(tmp_path, {
            "headwind.json": HEADWIND_SPEC,
            "tailwind.json": {**HEADWIND_SPEC, "desktop_dau": 0, "mobile_dau": 299000},
        })
        export.load_desktop_headwind_ramp(str(tmp_path), _index(), RAMP_START)
        assert "LEFT IN" not in capsys.readouterr().out

    def test_step_headwind_renders_through_the_package(self, tmp_path):
        """Any display-layer spec type the package can render is reversible here."""
        _write_specs(tmp_path, {"headwind.json": {
            "type": "step", "start_date": "2026-08-02", "desktop_dau": -1315000,
        }})
        ramp = export.load_desktop_headwind_ramp(str(tmp_path), _index(), RAMP_START)
        assert ramp[ANCHOR] == -1315000.0
        assert ramp[RAMP_START - pd.Timedelta(days=1)] == 0.0

    def test_raises_on_an_empty_directory(self, tmp_path):
        """An empty dir would make the output identical to the published file while its filename
        and column names still claimed the headwind had been removed."""
        (tmp_path / "empty").mkdir()
        with pytest.raises(FileNotFoundError, match="No adjustment specs"):
            export.load_desktop_headwind_ramp(str(tmp_path / "empty"), _index(), RAMP_START)

    def test_raises_when_the_headwind_spec_is_absent(self, tmp_path):
        _write_specs(tmp_path, {"tailwind.json": {**HEADWIND_SPEC, "desktop_dau": 0}})
        with pytest.raises(FileNotFoundError, match="no headwind.json"):
            export.load_desktop_headwind_ramp(str(tmp_path), _index(), RAMP_START)


class TestEndToEnd:
    """Round-trip a known pre-headwind curve through publish → strip and back."""

    @staticmethod
    def _published(tmp_path, raw_dec15=50_000_000.0):
        """Build a synthetic 'published' frame from a flat raw curve plus the real ramp.

        The raw curve is flat so the expected stripped value is a literal, not something recomputed
        by the same code path the test is checking.
        """
        index = _index()
        _write_specs(tmp_path / "aug", {"headwind.json": HEADWIND_SPEC})
        _write_specs(tmp_path / "jul", {"headwind.json": {
            **HEADWIND_SPEC, "start_date": "2026-04-01", "desktop_dau": -1345000,
        }})
        # The fixture follows the module's own seams so the round-trip stays valid across roll-forwards.
        aug_ramp = export.load_desktop_headwind_ramp(str(tmp_path / "aug"), index, export.FORECAST_START)
        jul_ramp = export.load_desktop_headwind_ramp(str(tmp_path / "jul"), index, export.PREV_FORECAST_START)
        raw = pd.Series(raw_dec15, index=index)
        published = pd.DataFrame({
            "desktop_actuals": raw.where(index < export.FORECAST_START),
            export.PRIOR_COLUMN: (raw + jul_ramp).round(0),
            export.CURRENT_COLUMN: (raw + aug_ramp).round(0).where(index >= export.FORECAST_START),
        }, index=index)
        return published, raw_dec15

    def test_strip_recovers_the_flat_pre_headwind_level(self, tmp_path, monkeypatch):
        published, raw_dec15 = self._published(tmp_path)
        monkeypatch.setattr(export, "CURRENT_ADJUSTMENTS_DIR", str(tmp_path / "aug"))
        monkeypatch.setattr(export, "PRIOR_ADJUSTMENTS_DIR", str(tmp_path / "jul"))

        curves, ramps = export.build_curves(published)
        stripped = curves.set_index(pd.to_datetime(curves["date"]))

        for column in [f"{export.PRIOR_COLUMN}_{export.LABEL}",
                       f"{export.CURRENT_COLUMN}_{export.LABEL}"]:
            values = stripped[column].dropna()
            assert (values - raw_dec15).abs().max() <= 1, (
                f"{column} did not return to the flat pre-headwind level")

    def test_actuals_pass_through_untouched(self, tmp_path, monkeypatch):
        published, _ = self._published(tmp_path)
        monkeypatch.setattr(export, "CURRENT_ADJUSTMENTS_DIR", str(tmp_path / "aug"))
        monkeypatch.setattr(export, "PRIOR_ADJUSTMENTS_DIR", str(tmp_path / "jul"))

        curves, _ = export.build_curves(published)
        pd.testing.assert_series_equal(
            curves.set_index(pd.to_datetime(curves["date"]))["desktop_actuals"],
            published["desktop_actuals"], check_names=False, check_freq=False,
        )

    def test_no_mobile_or_all_columns_survive(self, tmp_path, monkeypatch):
        published, _ = self._published(tmp_path)
        published["mobile_current_september"] = 17_000_000.0
        published["all_current_september"] = 67_000_000.0
        monkeypatch.setattr(export, "CURRENT_ADJUSTMENTS_DIR", str(tmp_path / "aug"))
        monkeypatch.setattr(export, "PRIOR_ADJUSTMENTS_DIR", str(tmp_path / "jul"))

        curves, _ = export.build_curves(published)
        assert list(curves.columns) == [
            "date", "desktop_actuals",
            f"{export.PRIOR_COLUMN}_{export.LABEL}", f"{export.CURRENT_COLUMN}_{export.LABEL}",
        ]

    def test_ledger_columns_recover_the_published_figures(self, tmp_path, monkeypatch):
        """The summary's win10_headwind_added_back_* columns are the audit trail back to the
        published numbers; if they disagree, a reader checking by hand finds a contradiction."""
        published, _ = self._published(tmp_path)
        monkeypatch.setattr(export, "CURRENT_ADJUSTMENTS_DIR", str(tmp_path / "aug"))
        monkeypatch.setattr(export, "PRIOR_ADJUSTMENTS_DIR", str(tmp_path / "jul"))

        curves, ramps = export.build_curves(published)
        row = export.build_summary(curves, ramps).iloc[0]

        assert row[f"win10_headwind_added_back_{export.CURRENT_KEY}"] == 1_315_000
        assert row[f"win10_headwind_added_back_{export.PRIOR_KEY}"] == 1_345_000
        recovered_aug = (row[f"current_{export.CURRENT_KEY}_{export.LABEL}"]
                         - row[f"win10_headwind_added_back_{export.CURRENT_KEY}"])
        recovered_jul = (row[f"prior_{export.PRIOR_KEY}_{export.LABEL}"]
                         - row[f"win10_headwind_added_back_{export.PRIOR_KEY}"])
        assert recovered_aug == published.loc[ANCHOR, export.CURRENT_COLUMN]
        assert recovered_jul == published.loc[ANCHOR, export.PRIOR_COLUMN]

    def test_summary_delta_equals_the_difference_of_its_own_columns(self, tmp_path, monkeypatch):
        published, _ = self._published(tmp_path)
        monkeypatch.setattr(export, "CURRENT_ADJUSTMENTS_DIR", str(tmp_path / "aug"))
        monkeypatch.setattr(export, "PRIOR_ADJUSTMENTS_DIR", str(tmp_path / "jul"))

        curves, ramps = export.build_curves(published)
        row = export.build_summary(curves, ramps).iloc[0]
        assert row[f"delta_vs_{export.PRIOR_KEY}_{export.LABEL}"] == (
            row[f"current_{export.CURRENT_KEY}_{export.LABEL}"] - row[f"prior_{export.PRIOR_KEY}_{export.LABEL}"])
