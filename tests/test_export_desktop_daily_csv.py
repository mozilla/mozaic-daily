"""Tests for scripts/export_desktop_daily_csv.py.

The daily export rests on one claim that is easy to break by "simplifying" the code: the Win10
headwind `h` is defined on the 28-day MA, and the only daily headwind consistent with it is the
ramp **advanced by 13.5 days**. Adding the published ramp per day looks right and re-smooths to
13.5 slopes above the published curve. These tests lock:

1. The advanced ramp inverts the trailing 28-day mean exactly from seam + 27, for an unclamped
   spec (August 2026) and for one that starts before its seam (July 2026, from 2026-04-01).
2. The unadvanced ramp fails that inversion, so the 13.5-day offset cannot be quietly dropped.
3. For a **clamped** spec (September 2026 on) the `exact` rule re-smooths to the flat MA through
   Dec-31, the `flat_at_anchor` rule only through the anchor, and the literal `advanced_clamped`
   rule is off by ~3.4 slopes at the anchor -- so the rule choice cannot be conflated.
4. `verify()` rejects a file built with the unadvanced ramp and accepts the advanced one, and
   reports a real discrepancy inside the `display_ma` transition window.

Unit tests run on synthetic series. One guarded integration test exercises the real September
and August builds if their parquets are present (gitignored, GCS-archived; skips in a clean checkout).
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

SEAM = export.FORECAST_START            # 2026-09-09
PREV_SEAM = export.PREV_FORECAST_START  # 2026-08-02
JULY_SEAM = pd.Timestamp("2026-07-06")
ANCHOR = pd.Timestamp("2026-12-15")
INDEX = pd.DatetimeIndex(pd.date_range(export.DISPLAY_START, export.DISPLAY_END))

SEPTEMBER_SPEC = {
    "type": "linear_ramp", "start_date": "2026-09-09", "anchor_date": "2026-12-15",
    "desktop_dau": -1017277, "mobile_dau": 0, "clamp_at_anchor": True,
}
AUGUST_SPEC = {
    "type": "linear_ramp", "start_date": "2026-08-02", "anchor_date": "2026-12-15",
    "desktop_dau": -1315000, "mobile_dau": -27162,
}
JULY_SPEC = {
    "type": "linear_ramp", "start_date": "2026-04-01", "anchor_date": "2026-12-15",
    "desktop_dau": -1345000, "mobile_dau": -27162,
}
SEPTEMBER_SLOPE = -1017277 / 97


def _synthetic_daily(seed: int = 0) -> pd.Series:
    """World desktop DAU with a ~40% weekend dip and drift, from 2024 through 2026."""
    index = pd.date_range("2024-01-01", "2026-12-31", freq="D")
    rng = np.random.default_rng(seed)
    weekday = np.where(index.dayofweek >= 5, 0.62, 1.0)
    drift = np.linspace(1.0, 1.05, len(index))
    return pd.Series(5.0e7 * weekday * drift + rng.normal(0, 20_000, len(index)), index=index)


def _first_full(seam: pd.Timestamp) -> pd.Timestamp:
    return seam + pd.Timedelta(days=export.MA_WINDOW - 1)


def _resmoothed_gap(spec: dict, seam: pd.Timestamp, rule: str) -> pd.Series:
    daily = export.advanced_daily_ramp(spec, INDEX, seam, rule)
    ma = export.ma_space_ramp(spec, INDEX, seam)
    return (daily.rolling(export.MA_WINDOW).mean() - ma)[_first_full(seam):]


class TestAdvancedDailyRampUnclamped:
    """The 13.5-day advance is what makes the daily file re-smooth to the published curve."""

    @pytest.mark.parametrize("spec, seam", [(AUGUST_SPEC, PREV_SEAM), (JULY_SPEC, JULY_SEAM)])
    def test_rolling_mean_reproduces_ma_space_ramp_after_transition(self, spec, seam):
        assert _resmoothed_gap(spec, seam, "exact").abs().max() < 1e-6

    def test_zero_before_the_seam(self):
        daily = export.advanced_daily_ramp(JULY_SPEC, INDEX, JULY_SEAM)
        assert (daily[: JULY_SEAM - pd.Timedelta(days=1)] == 0).all()

    def test_august_seam_day_value_is_13_5_slopes(self):
        """Slope 1,315,000 / 135 days; the seam-day value is 13.5 of them, not zero."""
        daily = export.advanced_daily_ramp(AUGUST_SPEC, INDEX, PREV_SEAM)
        assert daily[PREV_SEAM] == pytest.approx(-131_500.0, abs=0.5)

    def test_july_seam_day_carries_its_pre_seam_ramp_plus_the_advance(self):
        """July's spec starts 2026-04-01, so at its seam the MA-space ramp is already -500,465."""
        daily = export.advanced_daily_ramp(JULY_SPEC, INDEX, JULY_SEAM)
        ma = export.ma_space_ramp(JULY_SPEC, INDEX, JULY_SEAM)
        assert ma[JULY_SEAM] == pytest.approx(-1_345_000 * 96 / 258, abs=0.5)
        assert daily[JULY_SEAM] == pytest.approx(ma[JULY_SEAM] + 13.5 * (-1_345_000 / 258), abs=0.5)

    def test_post_anchor_rule_is_ignored_for_unclamped_specs(self):
        base = export.advanced_daily_ramp(AUGUST_SPEC, INDEX, PREV_SEAM, "exact")
        for rule in export.POST_ANCHOR_RULES:
            pd.testing.assert_series_equal(export.advanced_daily_ramp(AUGUST_SPEC, INDEX, PREV_SEAM, rule), base)

    def test_unadvanced_ramp_fails_the_inversion_check(self):
        """Adding the published ramp per day is the tempting mistake; it must be caught."""
        ma = export.ma_space_ramp(AUGUST_SPEC, INDEX, PREV_SEAM)
        with pytest.raises(AssertionError, match="does not invert"):
            export.check_ramp_inversion(ma.copy(), ma, PREV_SEAM)

    def test_non_linear_spec_is_refused(self, tmp_path):
        (tmp_path / "headwind.json").write_text(
            '{"type": "step", "start_date": "2026-08-02", "desktop_dau": -1000}')
        with pytest.raises(ValueError, match="linear_ramp"):
            export.load_win10_spec(str(tmp_path))


class TestAdvancedDailyRampClamped:
    """September's spec is flat after Dec-15; the three post-anchor rules differ only there."""

    def test_rules_agree_through_the_day_before_the_advance_reaches_the_anchor(self):
        cutoff = ANCHOR - pd.Timedelta(days=14)
        series = [export.advanced_daily_ramp(SEPTEMBER_SPEC, INDEX, SEAM, rule)[:cutoff]
                  for rule in export.POST_ANCHOR_RULES]
        for other in series[1:]:
            pd.testing.assert_series_equal(series[0], other)

    def test_exact_rule_resmooths_to_the_flat_ma_through_year_end(self):
        assert _resmoothed_gap(SEPTEMBER_SPEC, SEAM, "exact").abs().max() < 1e-6

    def test_exact_rule_repeats_with_period_28_after_the_anchor(self):
        daily = export.advanced_daily_ramp(SEPTEMBER_SPEC, INDEX, SEAM, "exact")
        after = daily[ANCHOR + pd.Timedelta(days=1):]
        assert np.allclose(after.to_numpy(), daily.shift(export.MA_WINDOW)[after.index].to_numpy())
        # The day after the anchor jumps back to the value 28 days earlier: 27 slopes shallower.
        assert daily[ANCHOR + pd.Timedelta(days=1)] - daily[ANCHOR] == pytest.approx(
            -27 * SEPTEMBER_SLOPE, abs=0.5)

    def test_exact_and_flat_rules_carry_13_5_slopes_past_the_anchor_on_dec_15(self):
        for rule in ("exact", "flat_at_anchor"):
            daily = export.advanced_daily_ramp(SEPTEMBER_SPEC, INDEX, SEAM, rule)
            assert daily[ANCHOR] == pytest.approx(-1_017_277 + 13.5 * SEPTEMBER_SLOPE, abs=0.5)

    def test_flat_rule_is_exact_through_the_anchor_then_too_deep(self):
        gap = _resmoothed_gap(SEPTEMBER_SPEC, SEAM, "flat_at_anchor")
        assert gap[:ANCHOR].abs().max() < 1e-6
        after = gap[ANCHOR + pd.Timedelta(days=1):]
        assert (after < -1_000).all()  # re-smoothed headwind deeper than published every day after
        assert export.exact_through(SEPTEMBER_SPEC, "flat_at_anchor") == ANCHOR

    def test_advanced_clamped_rule_is_shallow_at_the_anchor_by_3_5_slopes(self):
        """The 28-day mean of the clamped ramp, read 13.5 days ahead, misses the kink by 3.5 slopes.

        On the integer grid the window at Dec-15 holds 14 flat days and 14 ramp days; the ramp
        half averages 7 slopes above the anchor, so the whole window sits 3.5 slopes shallow.
        """
        gap = _resmoothed_gap(SEPTEMBER_SPEC, SEAM, "advanced_clamped")
        assert gap[ANCHOR] == pytest.approx(-3.5 * SEPTEMBER_SLOPE, abs=1.0)
        assert gap[: export.exact_through(SEPTEMBER_SPEC, "advanced_clamped")].abs().max() < 1e-6

    def test_inversion_check_uses_the_rule_specific_window(self):
        daily = export.advanced_daily_ramp(SEPTEMBER_SPEC, INDEX, SEAM, "flat_at_anchor")
        ma = export.ma_space_ramp(SEPTEMBER_SPEC, INDEX, SEAM)
        export.check_ramp_inversion(daily, ma, SEAM, through=ANCHOR)
        with pytest.raises(AssertionError, match="does not invert"):
            export.check_ramp_inversion(daily, ma, SEAM, through=export.DISPLAY_END)

    def test_unknown_rule_is_refused(self):
        with pytest.raises(ValueError, match="post_anchor_rule"):
            export.advanced_daily_ramp(SEPTEMBER_SPEC, INDEX, SEAM, "smooth_it_over")


def _published_and_file(daily_ramp_fn, tmp_path: Path, rule: str = "exact"):
    """A synthetic published MA file and the daily file some ramp function would produce."""
    model = _synthetic_daily()
    actuals = model[: SEAM - pd.Timedelta(days=1)]
    curves = pd.DataFrame(index=INDEX)
    curves[export.ACTUALS_COLUMN] = actuals.reindex(INDEX)
    published = pd.DataFrame(index=INDEX)
    published[export.ACTUALS_COLUMN] = actuals.rolling(export.MA_WINDOW).mean().reindex(INDEX)
    context = {"post_anchor_rule": rule, "ramps": {}}
    for column, spec, seam in [(export.CURRENT_COLUMN, SEPTEMBER_SPEC, SEAM),
                               (export.PRIOR_COLUMN, AUGUST_SPEC, PREV_SEAM)]:
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

    @pytest.mark.parametrize("rule", export.POST_ANCHOR_RULES)
    def test_accepts_each_rule_within_its_promised_window(self, tmp_path, rule):
        path, published, context = _published_and_file(
            lambda spec, seam, ma: export.advanced_daily_ramp(spec, INDEX, seam, rule), tmp_path, rule)
        transition_max = export.verify(path, published, context)
        # display_ma's splice is non-linear in the ramp, so the window is genuinely irreproducible.
        assert transition_max[export.CURRENT_COLUMN] > 1_000

    def test_rejects_flat_rule_file_when_exactness_is_claimed_through_year_end(self, tmp_path):
        path, published, context = _published_and_file(
            lambda spec, seam, ma: export.advanced_daily_ramp(spec, INDEX, seam, "flat_at_anchor"),
            tmp_path, rule="exact")
        with pytest.raises(AssertionError, match="differs from the published curve"):
            export.verify(path, published, context)

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
    reason="September/August desktop parquets not on disk (gitignored, GCS-archived)",
)
def test_real_builds_resmooth_to_the_published_file(tmp_path, monkeypatch):
    """End to end on the real September build: the file's rolling mean IS the published curve."""
    monkeypatch.chdir(REPO_ROOT)
    published = pd.read_csv(
        REPO_ROOT / export.CSV_DIR / export.PUBLISHED_CURVES, parse_dates=["date"]
    ).set_index("date")
    curves, context = export.build_curves()
    path = tmp_path / "daily.csv"
    curves.to_csv(path, index=False)
    transition_max = export.verify(path, published, context)
    frame = curves.set_index("date")
    # Dec-15 daily headwinds: September 13.5 slopes past its anchor, August 131,500 past its own.
    assert context["ramps"][export.CURRENT_COLUMN][0][ANCHOR] == pytest.approx(
        -1_017_277 + 13.5 * SEPTEMBER_SLOPE, abs=0.5)
    assert context["ramps"][export.PRIOR_COLUMN][0][ANCHOR] == pytest.approx(-1_446_500, abs=0.5)
    assert frame.loc[ANCHOR, export.CURRENT_COLUMN] > 5.0e7  # a Tuesday, unsmoothed
    # The prior column must be the August export byte-for-byte at Dec-15 (the number its README quotes).
    assert frame.loc[ANCHOR, export.PRIOR_COLUMN] == 55_077_204
    assert transition_max[export.CURRENT_COLUMN] > 10_000
