"""Tests for scripts/export_desktop_daily_csv.py.

The daily export rests on one claim that is easy to break by "simplifying" the code: the Win10
headwind `h` is defined on the 28-day MA, and the only daily headwind consistent with it is the
ramp **advanced by 13.5 days**. Adding the published ramp per day looks right and re-smooths to
131,500 DAU above the published curve. These tests lock:

1. The advanced ramp inverts the trailing 28-day mean exactly from seam + 27, for a spec that
   starts at the seam (August 2026) and one that starts earlier (July 2026, from 2026-04-01).
2. The unadvanced ramp fails that inversion, so the 13.5-day offset cannot be quietly dropped.
3. `verify()` rejects a file built with the unadvanced ramp and accepts the advanced one, and
   reports a real discrepancy inside the `display_ma` transition window.

Unit tests run on synthetic series. One guarded integration test exercises the real August and
July builds if their parquets are present (gitignored, GCS-archived; skips in a clean checkout).
"""

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "scripts"))
_spec = importlib.util.spec_from_file_location(
    "export_desktop_daily_csv", REPO_ROOT / "scripts" / "export_desktop_daily_csv.py"
)
export = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(export)

from mozaic_daily.seam_ma import display_ma  # noqa: E402

SEAM = export.FORECAST_START
PREV_SEAM = export.PREV_FORECAST_START
INDEX = pd.DatetimeIndex(pd.date_range(export.DISPLAY_START, export.DISPLAY_END))

AUGUST_SPEC = {
    "type": "linear_ramp", "start_date": "2026-08-02", "anchor_date": "2026-12-15",
    "desktop_dau": -1315000, "mobile_dau": -27162,
}
JULY_SPEC = {
    "type": "linear_ramp", "start_date": "2026-04-01", "anchor_date": "2026-12-15",
    "desktop_dau": -1345000, "mobile_dau": -27162,
}


def _synthetic_daily(seed: int = 0) -> pd.Series:
    """World desktop DAU with a ~40% weekend dip and drift, from 2024 through 2026."""
    index = pd.date_range("2024-01-01", "2026-12-31", freq="D")
    rng = np.random.default_rng(seed)
    weekday = np.where(index.dayofweek >= 5, 0.62, 1.0)
    drift = np.linspace(1.0, 1.05, len(index))
    return pd.Series(5.0e7 * weekday * drift + rng.normal(0, 20_000, len(index)), index=index)


def _first_full(seam: pd.Timestamp) -> pd.Timestamp:
    return seam + pd.Timedelta(days=export.MA_WINDOW - 1)


class TestAdvancedDailyRamp:
    """The 13.5-day advance is what makes the daily file re-smooth to the published curve."""

    @pytest.mark.parametrize("spec, seam", [(AUGUST_SPEC, SEAM), (JULY_SPEC, PREV_SEAM)])
    def test_rolling_mean_reproduces_ma_space_ramp_after_transition(self, spec, seam):
        daily = export.advanced_daily_ramp(spec, INDEX, seam)
        ma = export.ma_space_ramp(spec, INDEX, seam)
        residual = (daily.rolling(export.MA_WINDOW).mean() - ma)[_first_full(seam):].abs().max()
        assert residual < 1e-6

    def test_zero_before_the_seam(self):
        daily = export.advanced_daily_ramp(JULY_SPEC, INDEX, PREV_SEAM)
        assert (daily[: PREV_SEAM - pd.Timedelta(days=1)] == 0).all()

    def test_august_seam_day_value_is_13_5_slopes(self):
        """Slope 1,315,000 / 135 days; the seam-day value is 13.5 of them, not zero."""
        daily = export.advanced_daily_ramp(AUGUST_SPEC, INDEX, SEAM)
        assert daily[SEAM] == pytest.approx(-131_500.0, abs=0.5)

    def test_july_seam_day_carries_its_pre_seam_ramp_plus_the_advance(self):
        """July's spec starts 2026-04-01, so at its seam the MA-space ramp is already -500,465."""
        daily = export.advanced_daily_ramp(JULY_SPEC, INDEX, PREV_SEAM)
        ma = export.ma_space_ramp(JULY_SPEC, INDEX, PREV_SEAM)
        assert ma[PREV_SEAM] == pytest.approx(-1_345_000 * 96 / 258, abs=0.5)
        assert daily[PREV_SEAM] == pytest.approx(ma[PREV_SEAM] + 13.5 * (-1_345_000 / 258), abs=0.5)

    def test_unadvanced_ramp_fails_the_inversion_check(self):
        """Adding the published ramp per day is the tempting mistake; it must be caught."""
        ma = export.ma_space_ramp(AUGUST_SPEC, INDEX, SEAM)
        with pytest.raises(AssertionError, match="does not invert"):
            export.check_ramp_inversion(ma.copy(), ma, SEAM)

    def test_non_linear_spec_is_refused(self, tmp_path):
        (tmp_path / "headwind.json").write_text(
            '{"type": "step", "start_date": "2026-08-02", "desktop_dau": -1000}')
        with pytest.raises(ValueError, match="linear_ramp"):
            export.load_win10_spec(str(tmp_path))


def _published_and_file(daily_ramp_fn, tmp_path: Path):
    """A synthetic published MA file and the daily file some ramp function would produce."""
    model = _synthetic_daily()
    actuals = model[: SEAM - pd.Timedelta(days=1)]
    curves = pd.DataFrame(index=INDEX)
    curves[export.ACTUALS_COLUMN] = actuals.reindex(INDEX)
    published = pd.DataFrame(index=INDEX)
    published[export.ACTUALS_COLUMN] = actuals.rolling(export.MA_WINDOW).mean().reindex(INDEX)
    context = {"ramps": {}}
    for column, spec, seam in [(export.CURRENT_COLUMN, AUGUST_SPEC, SEAM),
                               (export.PRIOR_COLUMN, JULY_SPEC, PREV_SEAM)]:
        ma_ramp = export.ma_space_ramp(spec, INDEX, seam)
        daily_ramp = daily_ramp_fn(spec, seam, ma_ramp)
        # The published curve is display_ma of the model series plus the MA-space ramp.
        ma = display_ma(INDEX.to_series(), model.reindex(INDEX), seam)
        ma.index = INDEX
        published[column] = (ma + ma_ramp).round(0)
        col = model.reindex(INDEX) + daily_ramp
        col[INDEX < seam] = np.nan if column == export.CURRENT_COLUMN else col[INDEX < seam]
        curves[column] = col.round(0)
        context["ramps"][column] = (daily_ramp, ma_ramp, seam, spec)
    path = tmp_path / "daily.csv"
    curves.rename_axis("date").reset_index().to_csv(path, index=False)
    return path, published, context


class TestVerify:
    """`verify()` is what licenses shipping the file; it must reject the wrong ramp."""

    def test_accepts_advanced_ramp_and_reports_transition_discrepancy(self, tmp_path):
        path, published, context = _published_and_file(
            lambda spec, seam, ma: export.advanced_daily_ramp(spec, INDEX, seam), tmp_path)
        transition_max = export.verify(path, published, context)
        # display_ma's splice is non-linear in the ramp, so the window is genuinely irreproducible.
        assert transition_max[export.CURRENT_COLUMN] > 1_000

    def test_rejects_unadvanced_ramp(self, tmp_path):
        path, published, context = _published_and_file(lambda spec, seam, ma: ma, tmp_path)
        with pytest.raises(AssertionError, match="differs from the published curve"):
            export.verify(path, published, context)

    def test_rejects_leaked_mobile_column(self, tmp_path):
        path, published, context = _published_and_file(
            lambda spec, seam, ma: export.advanced_daily_ramp(spec, INDEX, seam), tmp_path)
        frame = pd.read_csv(path)
        frame["mobile_actuals"] = 1.0
        frame.to_csv(path, index=False)
        with pytest.raises(AssertionError, match="non-desktop columns"):
            export.verify(path, published, context)


@pytest.mark.skipif(
    not (REPO_ROOT / export.DESKTOP_FORECAST_PATH).exists()
    or not (REPO_ROOT / export.PREV_DESKTOP_FORECAST_PATH).exists(),
    reason="August/July desktop parquets not on disk (gitignored, GCS-archived)",
)
def test_real_builds_resmooth_to_the_published_file(tmp_path, monkeypatch):
    """End to end on the real August build: the file's rolling mean IS the published curve."""
    monkeypatch.chdir(REPO_ROOT)
    published = pd.read_csv(
        REPO_ROOT / export.CSV_DIR / export.PUBLISHED_CURVES, parse_dates=["date"]
    ).set_index("date")
    curves, context = export.build_curves()
    path = tmp_path / "daily.csv"
    curves.to_csv(path, index=False)
    transition_max = export.verify(path, published, context)
    frame = curves.set_index("date")
    # The daily Dec-15 value carries the advanced headwind, 131,500 deeper than the anchor.
    daily_ramp = context["ramps"][export.CURRENT_COLUMN][0]
    assert daily_ramp[export.MEASUREMENT_DATE] == pytest.approx(-1_446_500, abs=0.5)
    assert frame.loc[export.MEASUREMENT_DATE, export.CURRENT_COLUMN] > 5.0e7  # a Tuesday, unsmoothed
    assert transition_max[export.CURRENT_COLUMN] > 10_000
