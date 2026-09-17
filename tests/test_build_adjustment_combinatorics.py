"""Tests for the adoption helpers in ``scripts/build_adjustment_combinatorics.py``."""
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from build_adjustment_combinatorics import adopt_run, check_sidecar, parse_pairs  # noqa: E402
from mozaic.models import DesktopModelConfig  # noqa: E402

CONFIG = DesktopModelConfig()


def _run(tmp_path, *, seam="2026-09-09", config=None, codes=("i", "j")):
    tmp_path.mkdir(parents=True, exist_ok=True)
    parquet = tmp_path / f"mozaic_daily_forecast.{seam}.ld-D.adj-{''.join(codes)}.parquet"
    parquet.write_bytes(b"parquet-bytes")
    meta = {"forecast_start_date": seam, "model_config": (config or CONFIG).to_dict(),
            "adjustments_applied": [{"code": c} for c in codes], "artifact_sha1": "abc"}
    Path(str(parquet) + ".meta.json").write_text(json.dumps(meta))
    (tmp_path / "mozaic_objects.pkl").write_bytes(b"big")
    return parquet


def test_parse_pairs_rejects_a_bare_token():
    assert parse_pairs(["raw=a.parquet", "i+j=b.parquet"], "--reuse-run") == {"raw": Path("a.parquet"), "i+j": Path("b.parquet")}
    with pytest.raises(SystemExit, match="LABEL=PATH"):
        parse_pairs(["raw"], "--reuse-run")


def test_check_sidecar_accepts_a_match_and_names_each_mismatch(tmp_path):
    parquet = _run(tmp_path)
    meta = check_sidecar(parquet, forecast_start="2026-09-09", model_config=CONFIG.to_dict(), codes=["j", "i"], what="x")
    assert meta["artifact_sha1"] == "abc"
    other = DesktopModelConfig(prophet_changepoint_prior_scale=0.01).to_dict()
    with pytest.raises(SystemExit) as exc:
        check_sidecar(parquet, forecast_start="2026-09-02", model_config=other, codes=["i"], what="subset i")
    message = str(exc.value)
    assert "seam 2026-09-09 != 2026-09-02" in message
    assert "model_config differs" in message
    assert "codes ['i', 'j'] != ['i']" in message


def test_adopt_run_copies_parquet_and_sidecar_but_not_the_pickle(tmp_path):
    source = _run(tmp_path / "src")
    run_dir = tmp_path / "cache" / "i+j.key"
    target = adopt_run(source, run_dir, forecast_start="2026-09-09", config=CONFIG, enabled=frozenset("ij"))
    assert target == run_dir / source.name
    assert target.read_bytes() == b"parquet-bytes"
    assert Path(str(target) + ".meta.json").exists()
    assert not list(run_dir.glob("*.pkl"))
    params = json.loads((run_dir / "parameters.json").read_text())
    assert params["overlays_enabled"] == ["i", "j"]
    assert params["adopted_from"].endswith(source.name)
