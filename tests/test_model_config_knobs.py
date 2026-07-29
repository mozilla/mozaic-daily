"""Tests for the mozaic ``ModelConfig`` knobs this repo drives, focused on
``seasonality_corr_threshold``.

That knob moves the per-tile cutoff on desktop's level/volatility correlation
switch (``corr(|y|, |dy|) > threshold`` -> multiplicative + linear growth).
Because tiles are decided independently it is a continuous dial between
all-additive and all-multiplicative, unlike the 3-point ``seasonality_regime``.

The realistic regressions these guard against:
- the dataclass field is added but never threaded through the factory closure
  into ``desktop_forecast_model`` (a silent no-op -- the scan would report
  identical forecasts for every threshold);
- the slug stops distinguishing thresholds, so scan runs overwrite each other;
- a non-default threshold is passed on mobile, where there is no correlation
  cutoff for it to move, and is silently ignored.
"""

import numpy as np
import pandas as pd
import pytest

from mozaic import models as mozaic_models
from mozaic.models import DesktopModelConfig, MobileModelConfig, make_desktop_model


def test_threshold_defaults_to_legacy_hardcoded_zero():
    # 0.0 is the cutoff the model hardcoded before the knob existed; a change here
    # would silently alter every forecast that does not set the knob.
    assert DesktopModelConfig().seasonality_corr_threshold == 0.0


def test_threshold_reaches_desktop_forecast_model(monkeypatch):
    """The factory closure must forward the knob -- otherwise it is a no-op."""
    captured = {}

    def fake_forecast_model(*args, **kwargs):
        captured.update(kwargs)
        return None, None, None

    monkeypatch.setattr(mozaic_models, "desktop_forecast_model", fake_forecast_model)
    model = make_desktop_model(DesktopModelConfig(seasonality_corr_threshold=-0.15))
    model(pd.Series([1.0, 2.0]), pd.Series([0, 1]), pd.Series([2]))

    assert captured["seasonality_corr_threshold"] == -0.15


def test_slug_distinguishes_thresholds_and_omits_the_default():
    """Slugs name scan output dirs; colliding slugs overwrite results on disk."""
    default_slug = DesktopModelConfig().to_slug()
    assert "_corr" not in default_slug  # default stays absent for back-compat

    a = DesktopModelConfig(seasonality_corr_threshold=-0.15).to_slug()
    b = DesktopModelConfig(seasonality_corr_threshold=-0.26).to_slug()
    assert a != b != default_slug
    assert a.startswith(default_slug) and a.endswith("-0.15")


@pytest.mark.parametrize("bad", [-1.5, 1.5, 2.0])
def test_threshold_outside_correlation_range_is_rejected(bad):
    """It is a correlation cutoff; values outside [-1, 1] are a caller error."""
    series = pd.Series(np.linspace(1e6, 2e6, 60))
    dates = pd.Series(pd.date_range("2026-01-01", periods=60))
    with pytest.raises(AssertionError, match="must lie in"):
        mozaic_models.desktop_forecast_model(
            series, dates, pd.Series(pd.date_range("2026-03-01", periods=5)),
            seasonality_corr_threshold=bad,
        )


def test_mobile_rejects_a_non_default_threshold():
    """Mobile's regime switch is volume-driven, so the knob cannot apply there."""
    MobileModelConfig()  # default is fine
    with pytest.raises(ValueError, match="desktop-only"):
        MobileModelConfig(seasonality_corr_threshold=-0.15)


def test_threshold_flips_the_regime_decision_for_a_known_corr():
    """Behavioural check on the switch itself, via the params Prophet is built with.

    A series whose level and volatility correlate at a known negative value sits
    on the additive side of the legacy 0.0 cutoff and on the multiplicative side
    of a cutoff below it. Captures Prophet's constructor kwargs rather than
    fitting, so this stays fast.
    """
    rng = np.random.default_rng(0)
    n = 400
    # Rising level with volatility that *shrinks* as level grows -> negative corr.
    level = np.linspace(1e6, 4e6, n)
    noise = rng.normal(0, 1, n) * np.linspace(3e5, 1e4, n)
    series = pd.Series(level + noise)
    corr = series.abs().corr(series.diff().abs())
    assert corr < 0, f"fixture must have negative corr, got {corr}"

    seen = []

    class FakeProphet:
        def __init__(self, **kwargs):
            seen.append(kwargs)
            raise RuntimeError("stop after construction")

        add_seasonality = None

    dates = pd.Series(pd.date_range("2025-01-01", periods=n))
    future = pd.Series(pd.date_range("2026-02-05", periods=10))

    for threshold in (0.0, corr - 0.05):
        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(mozaic_models.prophet, "Prophet", FakeProphet)
            with pytest.raises(RuntimeError, match="stop after construction"):
                mozaic_models.desktop_forecast_model(
                    series, dates, future, seasonality_corr_threshold=threshold,
                )

    assert seen[0]["growth"] == "logistic"
    assert seen[0].get("seasonality_mode") != "multiplicative"
    assert seen[1]["growth"] == "linear"
    assert seen[1]["seasonality_mode"] == "multiplicative"
