import json
from pathlib import Path

import pandas as pd
import pytest

from spacex_etl import config, extract, load
from spacex_etl.quality import DataQualityError, assert_checks, run_checks
from spacex_etl.transform import transform_launches, transform_rockets

SAMPLE = Path(__file__).resolve().parents[1] / "sample_data"


@pytest.fixture
def raw():
    return (json.loads((SAMPLE / "launches.json").read_text()),
            json.loads((SAMPLE / "rockets.json").read_text()))


@pytest.fixture
def frames(raw):
    rockets = transform_rockets(raw[1])
    return transform_launches(raw[0], rockets), rockets


def test_transform_columns_and_status(frames):
    launches, rockets = frames
    assert {"launch_id", "rocket_name", "status", "year"} <= set(launches.columns)
    assert set(launches["status"]) <= {"success", "failure", "upcoming", "unknown"}
    assert launches["rocket_name"].notna().all()
    assert len(rockets) == 4


def test_quality_checks_pass_on_sample(frames):
    checks = run_checks(*frames)
    assert all(c["passed"] for c in checks), checks
    assert_checks(checks)


def test_quality_detects_duplicates(frames):
    launches, rockets = frames
    bad = pd.concat([launches, launches.head(1)])
    with pytest.raises(DataQualityError, match="launch_id_unico"):
        assert_checks(run_checks(bad, rockets))


def test_quality_detects_upcoming_with_result(frames):
    launches, rockets = frames
    bad = launches.copy()
    bad.loc[bad["upcoming"], "success"] = True
    assert not {c["check_name"]: c["passed"] for c in run_checks(bad, rockets)}["upcoming_sin_resultado"]


def test_extract_and_load_end_to_end(tmp_path, monkeypatch, frames):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "USE_SAMPLE_DATA", True)
    monkeypatch.setattr(config, "WAREHOUSE_URL", f"sqlite:///{tmp_path / 'wh.db'}")

    p = extract.extract("launches", "2026-10-29")
    assert Path(p).exists() and len(extract.read_json(p)) > 0

    launches, rockets = frames
    checks = run_checks(launches, rockets)
    load.save_quality_results(checks, "2026-10-29")
    load.save_quality_results(checks, "2026-10-29")  # idempotente
    assert load.load_tables(launches, rockets)["launches"] == len(launches)

    eng = load.get_engine()
    assert pd.read_sql("select count(*) n from quality_results", eng)["n"][0] == len(checks)
    assert pd.read_sql("select count(*) n from launches", eng)["n"][0] == len(launches)
