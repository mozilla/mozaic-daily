"""Tests for scripts/export_desktop_ex_ir_cn_csv.py.

The ex-IR/CN export rests on two claims that are easy to break by "simplifying" the code, so both
are locked here:

1. **`ALL` reconciles to the sum of its country tiles**, which is the only reason `ALL - IR - CN` is
   the true ex-scope rather than an approximation. Mozaic reconciles top-down, so this is a property
   of the build, not a guarantee — the tolerance must accept float noise and reject a real break.
2. **The MA is recomputed on the differenced DAILY series**, never by subtracting per-country MAs.
   `display_ma`'s variance-matched splice is non-linear, so the two disagree inside the 27 days after
   the seam. A future refactor that subtracts MAs would pass a naive equality test but ship wrong
   numbers in the splice window; `test_ma_of_difference_is_not_difference_of_mas` fails on it.

Unit tests run on synthetic pivots. One guarded integration test exercises the real August build if
its parquet is present (they are gitignored and GCS-archived, so it skips in a clean checkout).
"""

import importlib.util
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))
_spec = importlib.util.spec_from_file_location(
    "export_desktop_ex_ir_cn_csv", REPO_ROOT / "scripts" / "export_desktop_ex_ir_cn_csv.py"
)
export = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(export)

SEAM = export.FORECAST_START
PREV_SEAM = export.PREV_FORECAST_START
DEC15 = export.MEASUREMENT_DATE
COUNTRIES = ["US", "DE", "IR", "CN", "ROW"]

HEADWIND_SPEC = {
    "type": "linear_ramp", "start_date": "2026-08-02", "anchor_date": "2026-12-15",
    "desktop_dau": -1315000, "mobile_dau": -27162,
}
PRIOR_HEADWIND_SPEC = {
    "type": "linear_ramp", "start_date": "2026-04-01", "anchor_date": "2026-12-15",
    "desktop_dau": -1345000, "mobile_dau": -27162,
}


def _pivot(seed: int = 0, start="2024-01-01", end="2026-12-31") -> pd.DataFrame:
    """Synthetic per-country daily DAU with a weekly cycle, plus an exactly-reconciling ALL."""
    index = pd.date_range(start, end, freq="D")
    rng = np.random.default_rng(seed)
    levels = {"US": 20e6, "DE": 5e6, "IR": 1.0e6, "CN": 2.2e6, "ROW": 18e6}
    frame = pd.DataFrame(index=index)
    for i, country in enumerate(COUNTRIES):
        weekly = 1 + 0.04 * (i + 1) * np.sin(2 * np.pi * np.arange(len(index)) / 7)
        drift = np.linspace(1.0, 1.03, len(index))
        frame[country] = levels[country] * weekly * drift + rng.normal(0, 500, len(index))
    frame["ALL"] = frame[COUNTRIES].sum(axis=1)
    return frame


def _spec_dirs(tmp_path: Path) -> tuple[str, str]:
    for name, spec in [("aug", HEADWIND_SPEC), ("jul", PRIOR_HEADWIND_SPEC)]:
        (tmp_path / name).mkdir(parents=True, exist_ok=True)
        (tmp_path / name / "headwind.json").write_text(json.dumps(spec))
    return str(tmp_path / "aug"), str(tmp_path / "jul")


class TestCheckCountryReconciliation:
    """The guard that licenses treating ALL - IR - CN as exact."""

    def test_accepts_float_accumulation_noise(self):
        """Summing 16 float64 columns of ~5e7 leaves ~1e-8 of noise; exact equality is wrong."""
        pivot = _pivot()
        pivot["ALL"] += 3.7e-8
        residual = export.check_country_reconciliation(pivot, "synthetic")
        assert residual < export.RECONCILIATION_TOLERANCE_DAU

    def test_raises_on_a_real_reconciliation_break(self):
        """A top-down reconciliation failure is orders of magnitude above the float noise."""
        pivot = _pivot()
        pivot.loc[pivot.index[10], "ALL"] += 25_000
        with pytest.raises(ValueError, match="differs from the sum of its"):
            export.check_country_reconciliation(pivot, "synthetic")

    def test_tolerance_is_below_one_published_dau(self):
        """A residual that could move a rounded published figure must never pass."""
        assert export.RECONCILIATION_TOLERANCE_DAU < 0.5
        pivot = _pivot()
        pivot.loc[pivot.index[5], "ALL"] += 0.5
        with pytest.raises(ValueError):
            export.check_country_reconciliation(pivot, "synthetic")

    @pytest.mark.parametrize("missing", ["IR", "CN"])
    def test_raises_when_an_excluded_country_has_no_tile(self, missing):
        """If IR or CN is folded into ROW, the file cannot be produced -- it would silently
        under-exclude rather than fail."""
        pivot = _pivot().drop(columns=[missing])
        pivot["ALL"] = pivot[[c for c in COUNTRIES if c != missing]].sum(axis=1)
        with pytest.raises(ValueError, match="absent from the parquet's country tiles"):
            export.check_country_reconciliation(pivot, "synthetic")


class TestScopeDaily:
    def test_removes_exactly_ir_and_cn(self):
        pivot = _pivot()
        scoped = export.scope_daily(pivot)
        expected = pivot[["US", "DE", "ROW"]].sum(axis=1)
        assert (scoped - expected).abs().max() < 1e-6

    def test_keeps_the_row_bucket(self):
        """ROW holds every unlisted country. Dropping it would quietly remove ~36% of desktop."""
        pivot = _pivot()
        without_row = pivot.copy()
        without_row["ROW"] = 0.0
        without_row["ALL"] = without_row[COUNTRIES].sum(axis=1)
        assert export.scope_daily(pivot).mean() > export.scope_daily(without_row).mean() * 1.5


class TestMovingAverageMethod:
    """Locks the 'difference the daily series, then take the MA' contract."""

    def test_ma_of_difference_is_not_difference_of_mas(self):
        """The splice is non-linear, which is the whole reason this script reads the parquets.

        A refactor that subtracted per-country MAs would produce a materially different curve
        inside the 27 days after the seam. This test fails on that refactor.
        """
        pivot = _pivot()
        ma_of_difference = export.forecast_ma(export.scope_daily(pivot), SEAM)
        difference_of_mas = (
            export.forecast_ma(pivot["ALL"], SEAM)
            - export.forecast_ma(pivot["IR"], SEAM)
            - export.forecast_ma(pivot["CN"], SEAM)
        )
        splice = slice(SEAM, SEAM + pd.Timedelta(days=26))
        assert (ma_of_difference[splice] - difference_of_mas[splice]).abs().max() > 1.0

    def test_the_two_agree_well_past_the_seam(self):
        """Outside the splice `display_ma` is a plain rolling(28), which IS linear -- so Dec-15 and
        every headline number are unaffected by the choice. If this ever fails, the claim that only
        the splice window is at stake is wrong."""
        pivot = _pivot()
        ma_of_difference = export.forecast_ma(export.scope_daily(pivot), SEAM)
        difference_of_mas = (
            export.forecast_ma(pivot["ALL"], SEAM)
            - export.forecast_ma(pivot["IR"], SEAM)
            - export.forecast_ma(pivot["CN"], SEAM)
        )
        settled = slice(SEAM + pd.Timedelta(days=27), None)
        assert (ma_of_difference[settled] - difference_of_mas[settled]).abs().max() < 0.01


class TestScopeFrames:
    """The two output scopes and their relationship."""

    @staticmethod
    def _build(tmp_path, monkeypatch):
        august, july = _pivot(seed=1), _pivot(seed=2)
        aug_dir, jul_dir = _spec_dirs(tmp_path)
        monkeypatch.setattr(export, "CURRENT_ADJUSTMENTS_DIR", aug_dir)
        monkeypatch.setattr(export, "PRIOR_ADJUSTMENTS_DIR", jul_dir)
        training_dates = pd.DatetimeIndex(august.index[august.index < SEAM])
        curves, ramps = export.build_curves(august, july, training_dates)
        frames = {s["key"]: export.build_scope_frame(s, curves, ramps) for s in export.SCOPES}
        return frames, curves, ramps

    def test_scopes_differ_by_exactly_the_ramp(self, tmp_path, monkeypatch):
        frames, _, ramps = self._build(tmp_path, monkeypatch)
        canonical = frames["canonical"].set_index(pd.to_datetime(frames["canonical"]["date"]))
        no_hw = frames["no_headwind"].set_index(pd.to_datetime(frames["no_headwind"]["date"]))
        for family, canonical_suffix, no_hw_suffix in [
            ("current_august", export.SCOPE_LABEL, f"NO_WIN10_HEADWIND_{export.SCOPE_LABEL}"),
            ("prior_july", export.SCOPE_LABEL, f"NO_WIN10_HEADWIND_{export.SCOPE_LABEL}"),
        ]:
            difference = (canonical[f"desktop_{family}_{canonical_suffix}"]
                          - no_hw[f"desktop_{family}_{no_hw_suffix}"])
            expected = ramps[family].reindex(canonical.index)
            overlap = difference.notna() & expected.notna()
            assert overlap.sum() > 100, "no overlapping rows to compare"
            assert (difference[overlap] - expected[overlap]).abs().max() <= 1

    def test_canonical_is_below_no_headwind(self, tmp_path, monkeypatch):
        """`h` is negative, so the canonical curve must sit BELOW the no-headwind one. A sign flip
        here would publish the headwind as a tailwind."""
        frames, _, _ = self._build(tmp_path, monkeypatch)
        canonical = frames["canonical"].set_index(pd.to_datetime(frames["canonical"]["date"]))
        no_hw = frames["no_headwind"].set_index(pd.to_datetime(frames["no_headwind"]["date"]))
        column = f"desktop_current_august_{export.SCOPE_LABEL}"
        no_hw_column = f"desktop_current_august_NO_WIN10_HEADWIND_{export.SCOPE_LABEL}"
        assert canonical.loc[DEC15, column] < no_hw.loc[DEC15, no_hw_column]

    def test_column_names_and_no_world_or_mobile_leakage(self, tmp_path, monkeypatch):
        frames, _, _ = self._build(tmp_path, monkeypatch)
        assert list(frames["canonical"].columns) == [
            "date", f"desktop_actuals_{export.SCOPE_LABEL}",
            f"desktop_prior_july_{export.SCOPE_LABEL}",
            f"desktop_current_august_{export.SCOPE_LABEL}",
        ]
        assert list(frames["no_headwind"].columns) == [
            "date", f"desktop_actuals_{export.SCOPE_LABEL}",
            f"desktop_prior_july_NO_WIN10_HEADWIND_{export.SCOPE_LABEL}",
            f"desktop_current_august_NO_WIN10_HEADWIND_{export.SCOPE_LABEL}",
        ]
        for frame in frames.values():
            assert not [c for c in frame.columns if "mobile" in c or c.startswith("all_")]

    def test_actuals_are_identical_across_scopes_and_carry_no_headwind_label(
            self, tmp_path, monkeypatch):
        """Actuals never carry an adjustment, so labelling them NO_WIN10_HEADWIND would assert
        something meaningless -- and they must not differ between the two files."""
        frames, _, _ = self._build(tmp_path, monkeypatch)
        column = f"desktop_actuals_{export.SCOPE_LABEL}"
        assert column in frames["canonical"].columns
        assert column in frames["no_headwind"].columns
        pd.testing.assert_series_equal(
            frames["canonical"][column], frames["no_headwind"][column], check_names=False)

    def test_forecast_column_is_blank_before_the_seam(self, tmp_path, monkeypatch):
        frames, _, _ = self._build(tmp_path, monkeypatch)
        frame = frames["canonical"].set_index(pd.to_datetime(frames["canonical"]["date"]))
        column = f"desktop_current_august_{export.SCOPE_LABEL}"
        assert frame[column].first_valid_index() == SEAM


class TestScopeSummary:
    def _summary(self, tmp_path, monkeypatch, scope_key):
        frames, _, ramps = TestScopeFrames._build(tmp_path, monkeypatch)
        scope = next(s for s in export.SCOPES if s["key"] == scope_key)
        return export.build_scope_summary(scope, frames[scope_key], ramps).iloc[0], scope

    def test_delta_equals_difference_of_its_own_columns(self, tmp_path, monkeypatch):
        row, scope = self._summary(tmp_path, monkeypatch, "canonical")
        suffix = scope["column_suffix"]
        assert row[f"delta_vs_july_{suffix}"] == (
            row[f"current_august_{suffix}"] - row[f"prior_july_{suffix}"])

    def test_ledger_sign_is_negative_where_the_headwind_is_applied(self, tmp_path, monkeypatch):
        """The canonical file HAS -1,315,000 applied; the no-headwind file needs +1,315,000 added
        back. Getting the sign wrong would invert the audit trail between the two files."""
        canonical, _ = self._summary(tmp_path, monkeypatch, "canonical")
        no_headwind, _ = self._summary(tmp_path, monkeypatch, "no_headwind")
        assert canonical["win10_headwind_applied_august"] == -1_315_000
        assert canonical["win10_headwind_applied_july"] == -1_345_000
        assert no_headwind["win10_headwind_added_back_august"] == 1_315_000
        assert no_headwind["win10_headwind_added_back_july"] == 1_345_000

    def test_ledger_recovers_the_canonical_figure_from_the_no_headwind_one(
            self, tmp_path, monkeypatch):
        canonical, canonical_scope = self._summary(tmp_path, monkeypatch, "canonical")
        no_headwind, no_hw_scope = self._summary(tmp_path, monkeypatch, "no_headwind")
        recovered = (no_headwind[f"current_august_{no_hw_scope['column_suffix']}"]
                     - no_headwind["win10_headwind_added_back_august"])
        assert abs(recovered - canonical[f"current_august_{canonical_scope['column_suffix']}"]) <= 1


@pytest.mark.skipif(
    not (REPO_ROOT / export.DESKTOP_FORECAST_PATH).exists(),
    reason="August desktop parquet is gitignored / GCS-archived",
)
class TestAgainstTheRealBuild:
    """Guarded checks against the on-disk August build — the properties the output actually rests on."""

    def test_world_reconstruction_matches_the_published_csv(self):
        """The load-bearing property: if the world curve rebuilt from the parquet reproduces the
        published CSV, the only thing separating the ex-IR/CN file from it is the subtraction."""
        published = pd.read_csv(
            REPO_ROOT / export.CSV_DIR / export.PUBLISHED_CURVES, parse_dates=["date"]
        ).set_index("date")
        august, _ = export.load_country_dau(str(REPO_ROOT / export.DESKTOP_FORECAST_PATH))
        july, _ = export.load_country_dau(str(REPO_ROOT / export.PREV_DESKTOP_FORECAST_PATH))
        export.verify_world_reconstruction(august, july, published, august.index)

    def test_ir_training_rows_are_real_telemetry_not_the_counterfactual_fill(self):
        """The actuals column is built from training rows, which is only valid because they are real
        telemetry. IR's shutdown crater is the sharpest available probe: the fill is smooth there,
        the actuals collapse to ~6K. If mozaic ever writes filled values into the parquet's training
        rows, this fails and the actuals column silently stops being actuals."""
        august, _ = export.load_country_dau(str(REPO_ROOT / export.DESKTOP_FORECAST_PATH))
        crater = august.loc["2026-03-01":"2026-03-05", "IR"]
        pre_shutdown = august.loc["2026-02-20":"2026-02-25", "IR"].mean()
        assert crater.max() < 50_000, (
            f"IR peaks at {crater.max():,.0f} DAU during the 2026-03 shutdown; the real crater was "
            f"~6K. Training rows appear to carry the counterfactual fill, not actuals."
        )
        assert pre_shutdown > 500_000, "IR pre-shutdown level looks wrong; check the selector"

    def test_excluded_countries_are_a_material_share(self):
        """Locks the ~5.8% scope difference. A near-zero difference would mean the subtraction
        silently did nothing."""
        august, _ = export.load_country_dau(str(REPO_ROOT / export.DESKTOP_FORECAST_PATH))
        world = export.forecast_ma(august["ALL"], SEAM)[DEC15]
        scoped = export.forecast_ma(export.scope_daily(august), SEAM)[DEC15]
        share = 1 - scoped / world
        assert 0.04 < share < 0.08, f"IR+CN are {share:.2%} of desktop at Dec-15, expected ~5.8%"
