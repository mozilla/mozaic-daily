"""Contract tests for the PLACEHOLDER MozillaOnline migration model artifact.

These guard the *output contract* the `o` adjustment applier (and any swap of
Brad's official model) will depend on. They exercise the on-disk artifacts
produced by ``data-official/2026-07/mozillaonline/build_placeholder_model.py``.

Each assertion catches a realistic regression in the generator:
- a broken/duplicated/unsorted date index would break the loader,
- a NaN or missing value column would corrupt the subtract/add-back,
- non-zero pre-migration values would inject a phantom adjustment,
- shares not summing to 1.0 (or CN < 0.90) would mis-allocate the overlay,
- a curve that never rises then falls would mean the model didn't build.
"""
import json
from pathlib import Path

import pandas as pd
import pytest

MODEL_DIR = Path(__file__).resolve().parents[1] / "data-official" / "2026-07" / "mozillaonline"
SPEC_PATH = MODEL_DIR / "mozillaonline.json"
RELEASE_START = pd.Timestamp("2026-06-02")

pytestmark = pytest.mark.skipif(
    not SPEC_PATH.exists(),
    reason="MozillaOnline placeholder artifacts not built (run build_placeholder_model.py)",
)


@pytest.fixture(scope="module")
def spec():
    return json.loads(SPEC_PATH.read_text())


@pytest.fixture(scope="module")
def model_df(spec):
    return pd.read_parquet(MODEL_DIR / spec["data_file"])


def test_spec_points_at_real_files(spec):
    assert spec["type"] == "mozillaonline_migration"
    assert spec["platform"] == "desktop"
    assert (MODEL_DIR / spec["data_file"]).exists()
    assert (MODEL_DIR / spec["model_meta_file"]).exists()


def test_index_is_clean_daily_datetime(model_df):
    idx = model_df.index
    assert idx.name == "target_date"
    assert isinstance(idx, pd.DatetimeIndex)
    assert idx.is_unique
    assert idx.is_monotonic_increasing
    # midnight-normalized
    assert (idx.normalize() == idx).all()
    # contiguous daily
    assert (idx.to_series().diff().dropna() == pd.Timedelta(days=1)).all()


def test_value_columns_present_and_finite(model_df, spec):
    vc = spec["value_column"]
    assert vc == "migration_dau_daily"
    assert vc in model_df.columns
    assert "migration_dau_ma" in model_df.columns
    assert not model_df[vc].isna().any()
    assert (model_df[vc] >= 0).all()


def test_zero_before_migration_start(model_df):
    pre = model_df.loc[: RELEASE_START - pd.Timedelta(days=1), "migration_dau_daily"]
    assert pre.abs().max() == 0, "overlay must be zero before the first cohort starts"


def test_shape_rises_to_peak_then_declines(model_df):
    s = model_df["migration_dau_daily"]
    peak_date = s.idxmax()
    assert s.max() > 100_000, "model should reach a substantial peak"
    # rises into the peak
    assert s.loc[RELEASE_START] < s.loc[peak_date]
    # declines after the peak (churn), allowing tiny numerical noise
    tail = s.loc[peak_date:]
    assert tail.iloc[-1] < tail.iloc[0], "post-peak churn decline expected"


def test_geo_shares_sum_to_one_cn_dominant(spec):
    shares = spec["allocation"]["shares"]
    assert abs(sum(shares.values()) - 1.0) < 1e-6
    assert shares["CN"] >= 0.90
    assert spec["allocation"]["key"] == "fixed_country_shares"
    assert spec["scope"]["exclude_countries"] == ["IR"]


def test_meta_flags_placeholder_and_swap(spec):
    meta = json.loads((MODEL_DIR / spec["model_meta_file"]).read_text())
    assert meta["placeholder"] is True
    assert spec["placeholder"] is True
    assert "swap_instructions" in meta and meta["swap_instructions"]
