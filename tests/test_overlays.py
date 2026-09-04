"""Tests for ``mozaic_daily.overlays`` — registry-driven per-tile overlay dispatch.

The failure modes worth testing are the ones that would silently apply the wrong set of
overlays: a registry entry left out of dispatch, a spec gating on the wrong date, a disabled
code still applied, a curve allocated to an excluded country, or two overlays colliding on
the same idempotency sentinel. Fixtures build a throwaway repo root under ``tmp_path`` so no
test depends on the real cycle directories — except the last class, which checks that the
committed registry and specs still resolve the way the August build did.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from mozaic_daily import overlays
from mozaic_daily.adjustments import load_code_registry
from mozaic_daily.overlays import (
    ResolvedOverlay,
    add_overlays_post_mozaic,
    find_spec_for_forecast,
    overlay_country_shares,
    registered_overlay_codes,
    resolve_overlays,
    subtract_overlays_pre_mozaic,
)
from mozaic_daily.queries import DataSource
from tests.test_adjustments import _make_desktop_forecast_fixture, _make_desktop_training_fixture

# Aligned with the desktop fixtures in tests/test_adjustments.py (training 03-15..05-13,
# forecast rows 03-15..06-15).
SEAM = "2026-05-14"
TRAINING_END = pd.Timestamp("2026-05-13")
CURVE_START = pd.Timestamp("2026-04-15")


def _registry(**codes) -> dict:
    """Minimal registry dict; each value is ``(applier, spec_glob)``."""
    return {
        code: {"name": f"{code}_overlay", "applier": applier, "description": "test", "spec_glob": glob}
        for code, (applier, glob) in codes.items()
    }


def _write_curve(path: Path, value: float, column: str = "curve_dau_daily") -> None:
    idx = pd.date_range("2026-01-01", "2027-12-31", freq="D", name="target_date")
    daily = pd.Series(0.0, index=idx)
    daily[idx >= CURVE_START] = value
    pd.DataFrame({column: daily, column.replace("daily", "ma"): daily}).to_parquet(path)


def _write_spec(root: Path, dirname: str, *, applies_to: str = SEAM, allocation: dict | None = None,
                exclude: list[str] | None = None, data_source: str = "legacy_desktop", value: float = 1000.0) -> Path:
    spec_dir = root / "data-official" / "2026-09" / dirname
    spec_dir.mkdir(parents=True, exist_ok=True)
    _write_curve(spec_dir / "curve.parquet", value)
    spec = {
        "type": "desktop_overlay",
        "platform": "desktop",
        "data_file": "curve.parquet",
        "value_column": "curve_dau_daily",
        "allocation": allocation or {"key": "trailing_dau_share", "flag_column": "modern_windows", "window_days": 28},
        "scope": {"exclude_countries": exclude or []},
        "applies_to_forecast_start": applies_to,
        "applies_to_data_source": data_source,
    }
    path = spec_dir / f"{dirname}.json"
    path.write_text(json.dumps(spec))
    return path


@pytest.fixture
def root(tmp_path) -> Path:
    return tmp_path


# --- registry ---------------------------------------------------------------

class TestRegisteredOverlayCodes:
    def test_selects_only_per_tile_overlay_entries(self):
        registry = _registry(h=("display_layer", "x"), l=("per_tile_overlay", "y"),
                             p=("paid_organic_split", "z"), m=("marketing_lift", "w"))
        assert list(registered_overlay_codes(registry)) == ["l"]

    def test_missing_applier_raises(self):
        registry = _registry(l=("per_tile_overlay", "y"))
        del registry["l"]["applier"]
        with pytest.raises(ValueError, match="applier=None"):
            registered_overlay_codes(registry)

    def test_unknown_applier_raises(self):
        with pytest.raises(ValueError, match="applier='magic'"):
            registered_overlay_codes(_registry(q=("magic", "y")))


# --- spec finding -----------------------------------------------------------

class TestFindSpecForForecast:
    def test_exact_date_match_only(self, root):
        _write_spec(root, "foo", applies_to=SEAM)
        assert find_spec_for_forecast("data-official/*/foo/foo.json", SEAM, "foo", root) is not None
        assert find_spec_for_forecast("data-official/*/foo/foo.json", "2026-09-03", "foo", root) is None

    def test_null_gate_never_matches(self, root):
        path = _write_spec(root, "foo")
        spec = json.loads(path.read_text())
        spec["applies_to_forecast_start"] = None
        path.write_text(json.dumps(spec))
        assert find_spec_for_forecast("data-official/*/foo/foo.json", SEAM, "foo", root) is None

    def test_two_specs_claiming_one_date_raise(self, root):
        _write_spec(root, "foo")
        other = root / "data-official" / "2026-10" / "foo"
        other.mkdir(parents=True)
        (other / "foo.json").write_text(json.dumps({"applies_to_forecast_start": SEAM}))
        with pytest.raises(ValueError, match="Multiple foo specs"):
            find_spec_for_forecast("data-official/*/foo/foo.json", SEAM, "foo", root)


# --- resolution -------------------------------------------------------------

class TestResolveOverlays:
    def _registry(self):
        return _registry(l=("per_tile_overlay", "data-official/*/lol/lol.json"),
                         j=("per_tile_overlay", "data-official/*/jbot/jbot.json"),
                         h=("display_layer", "data-official/*/adjustments/headwind.json"))

    def test_returns_gating_overlays_sorted_by_code(self, root):
        _write_spec(root, "lol")
        _write_spec(root, "jbot", allocation={"key": "fixed_country_shares", "flag_column": "modern_windows",
                                              "window_days": 28, "shares": {"JP": 1.0}})
        resolved = resolve_overlays(SEAM, registry=self._registry(), root=root)
        assert [o.code for o in resolved] == ["j", "l"]
        assert resolved[1].name == "l_overlay"
        assert resolved[1].sentinel_attr == "l_overlay_subtracted"
        assert resolved[0].data_source == DataSource.LEGACY_DESKTOP

    def test_disabled_code_is_skipped_even_when_its_spec_matches(self, root):
        _write_spec(root, "lol")
        assert resolve_overlays(SEAM, disabled_codes={"l"}, registry=self._registry(), root=root) == []

    def test_spec_on_another_date_is_not_resolved(self, root):
        _write_spec(root, "lol", applies_to="2026-08-02")
        assert resolve_overlays(SEAM, registry=self._registry(), root=root) == []

    def test_unknown_data_source_raises(self, root):
        _write_spec(root, "lol", data_source="glean_toaster")
        with pytest.raises(ValueError, match="applies_to_data_source='glean_toaster'"):
            resolve_overlays(SEAM, registry=self._registry(), root=root)

    def test_unknown_allocation_key_raises(self, root):
        _write_spec(root, "lol", allocation={"key": "vibes", "flag_column": "modern_windows", "window_days": 28})
        with pytest.raises(ValueError, match="allocation.key='vibes'"):
            resolve_overlays(SEAM, registry=self._registry(), root=root)

    def test_fixed_shares_without_shares_dict_raises(self, root):
        _write_spec(root, "lol", allocation={"key": "fixed_country_shares", "flag_column": "modern_windows",
                                             "window_days": 28})
        with pytest.raises(ValueError, match="no allocation.shares"):
            resolve_overlays(SEAM, registry=self._registry(), root=root)


# --- allocation -------------------------------------------------------------

def _overlay(root: Path, dirname: str, **kwargs) -> ResolvedOverlay:
    path = _write_spec(root, dirname, **kwargs)
    return ResolvedOverlay(code=dirname[0], name=dirname, spec_path=path, spec=json.loads(path.read_text()))


class TestOverlayCountryShares:
    def test_trailing_share_honours_exclusions_and_renormalizes(self, root):
        training = _make_desktop_training_fixture()
        overlay = _overlay(root, "lol", exclude=["US"])
        shares = overlay_country_shares(overlay, training, TRAINING_END)
        assert "US" not in shares.index
        assert shares.sum() == pytest.approx(1.0)
        unexcluded = overlay_country_shares(_overlay(root, "lol2"), training, TRAINING_END)
        assert "US" in unexcluded.index

    def test_fixed_shares_dispatch(self, root):
        training = _make_desktop_training_fixture()
        overlay = _overlay(root, "jbot", allocation={"key": "fixed_country_shares", "flag_column": "modern_windows",
                                                     "window_days": 28, "shares": {"JP": 0.5, "US": 0.5, "ZZ": 0.2}})
        shares = overlay_country_shares(overlay, training, TRAINING_END)
        present = set(training["country"].unique())
        assert set(shares.index) <= present
        assert shares.sum() == pytest.approx(1.0)


# --- apply -----------------------------------------------------------------

class TestSubtractAndAddBack:
    def test_two_overlays_stack_with_distinct_sentinels_and_round_trip_at_world(self, root):
        training = _make_desktop_training_fixture()
        first = _overlay(root, "lol", value=1000.0)
        second = _overlay(root, "jbot", value=500.0, allocation={
            "key": "fixed_country_shares", "flag_column": "modern_windows", "window_days": 28, "shares": {"US": 1.0}})
        source = {"DAU": training}
        modified, contexts = subtract_overlays_pre_mozaic(source, [second, first], str(TRAINING_END.date()))
        assert modified["DAU"].attrs["lol_subtracted"] and modified["DAU"].attrs["jbot_subtracted"]
        assert [c["overlay"].code for c in contexts] == ["j", "l"]

        in_curve = pd.to_datetime(modified["DAU"]["x"]) >= CURVE_START
        flagged = modified["DAU"]["modern_windows"] == True  # noqa: E712
        removed = training.loc[in_curve & flagged, "y"].sum() - modified["DAU"].loc[in_curve & flagged, "y"].sum()
        days_in_curve = pd.to_datetime(training.loc[flagged, "x"]).unique()
        days_in_curve = days_in_curve[days_in_curve >= CURVE_START]
        assert removed == pytest.approx(1500.0 * len(days_in_curve), rel=0.01)

        forecast = _make_desktop_forecast_fixture()
        added = add_overlays_post_mozaic(forecast, contexts, pd.Timestamp(SEAM))
        world = (added["country"] == "ALL") & (added["population"] == "ALL")
        dates = pd.to_datetime(added.loc[world, "target_date"])
        delta = (added.loc[world, "DAU"] - forecast.loc[world, "DAU"])
        assert (delta[dates >= CURVE_START] == 1500.0).all()  # 1000 (l) + 500 (j), exact on the world row
        assert (delta[dates < CURVE_START] == 0).all()

    def test_same_overlay_twice_is_refused(self, root):
        training = _make_desktop_training_fixture()
        overlay = _overlay(root, "lol")
        with pytest.raises(RuntimeError):
            subtract_overlays_pre_mozaic({"DAU": training}, [overlay, overlay], str(TRAINING_END.date()))

    def test_source_without_dau_passes_through_add_back(self, root):
        frame = pd.DataFrame({"country": ["ALL"], "population": ["ALL"], "target_date": [SEAM], "source": ["forecast"]})
        assert add_overlays_post_mozaic(frame, [], pd.Timestamp(SEAM)) is frame


# --- the committed repo -----------------------------------------------------

class TestCommittedRegistryAndSpecs:
    def test_every_registered_code_declares_a_known_applier(self):
        registry = load_code_registry()
        assert set(registry) >= {"h", "m", "p", "l", "o", "t"}
        assert registered_overlay_codes(registry).keys() >= {"l", "o"}
        assert set(registered_overlay_codes(registry)).isdisjoint({"h", "t", "m", "p"})

    def test_august_seam_resolves_to_l_and_o(self):
        resolved = resolve_overlays("2026-08-02")
        assert [o.code for o in resolved] == ["l", "o"]
        assert {o.data_source for o in resolved} == {DataSource.LEGACY_DESKTOP}
        assert resolved[0].sentinel_attr == "launch_on_login_new_users_subtracted"  # renamed 2026-09-04; code + layout unchanged

    def test_september_seam_carries_launch_at_login_forward(self):
        """`l` was re-gated to 2026-09-02 on 2026-09-04 with August's 200K curve unchanged (retitled 'new users')."""
        sep = {o.code: o for o in resolve_overlays("2026-09-02")}
        aug = {o.code: o for o in resolve_overlays("2026-08-02")}
        assert sep["l"].name == "launch_on_login_new_users"
        assert sep["l"].spec["data_file"] == aug["l"].spec["data_file"] == "lol_tailwind.2026-07-29.cap200k.parquet"
        assert sep["l"].spec_path.parent.name == "launch_on_login"

    def test_september_seam_resolves_japan_bot(self):
        """`j` was the first code wired purely through the registry (2026-09-04)."""
        by_code = {o.code: o for o in resolve_overlays("2026-09-02")}
        assert "j" in by_code
        assert by_code["j"].data_source == DataSource.LEGACY_DESKTOP
        assert by_code["j"].sentinel_attr == "japan_bot_subtracted"
        assert by_code["j"].spec["allocation"]["shares"] == {"JP": 1.0}

    def test_september_seam_resolves_india_excess(self):
        """`i` registered 2026-09-04; ships the PROPORTIONAL path, 100% India, net of `l`."""
        by_code = {o.code: o for o in resolve_overlays("2026-09-02")}
        assert "i" in by_code
        assert by_code["i"].data_source == DataSource.LEGACY_DESKTOP
        assert by_code["i"].sentinel_attr == "india_excess_subtracted"
        assert by_code["i"].spec["allocation"]["shares"] == {"IN": 1.0}
        assert ".proportional." in by_code["i"].spec["data_file"]

    def test_september_seam_overlays_have_distinct_sentinels(self):
        """Every overlay gated on the September seam subtracts under its own sentinel.

        `i` and `j` both resolve today; `l` and `o` join once their September specs are
        re-gated (the `i` curve is already net of `l`, so `l` must be applied in the same run).
        """
        resolved = resolve_overlays("2026-09-02")
        assert {o.code for o in resolved} >= {"j", "i"}
        sentinels = [o.sentinel_attr for o in resolved]
        assert len(sentinels) == len(set(sentinels))

    def test_september_seam_resolves_india_excess(self):
        by_code = {o.code: o for o in resolve_overlays("2026-09-02")}
        assert "i" in by_code
        assert by_code["i"].sentinel_attr == "india_excess_subtracted"
        assert by_code["i"].spec["allocation"]["shares"] == {"IN": 1.0}
        assert "proportional" in by_code["i"].spec["notes"].lower()

    def test_september_seam_resolves_refreshed_mozillaonline(self):
        """`o` was refreshed for September (2026-09-04); August's frozen curve must still gate on 2026-08-02."""
        sep = {o.code: o for o in resolve_overlays("2026-09-02")}
        aug = {o.code: o for o in resolve_overlays("2026-08-02")}
        assert sep["o"].spec["data_file"] == "mozillaonline_migration.2026-08-31.parquet"
        assert sep["o"].spec_path.parent.name == "mozillaonline"
        assert aug["o"].spec["data_file"] == "mozillaonline_migration_model.official.2026-06-29.parquet"
        assert sep["o"].spec["allocation"]["shares"]["CN"] == 0.9277

    def test_repo_root_points_at_the_checkout(self):
        assert (overlays.repo_root() / "data-official" / "adjustment_codes.yaml").exists()
