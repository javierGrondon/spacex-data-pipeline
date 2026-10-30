from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_spacex_dag_structure(dagbag):
    dag = dagbag.get_dag("spacex_pipeline")
    assert dag is not None
    assert {t.task_id for t in dag.tasks} == {
        "api_disponible", "extract_launches", "extract_rockets", "transform",
        "quality_checks", "load", "enviar_reporte",
    }
    assert dag.get_task("load").downstream_task_ids == {"enviar_reporte"}
    assert dag.catchup is False


def test_alerts_configured(dagbag):
    dag = dagbag.get_dag("spacex_pipeline")
    for t in dag.tasks:
        assert t.on_failure_callback, f"{t.task_id} sin alerta de falla"
        assert t.retries >= 1
