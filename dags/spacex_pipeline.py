"""
SpaceX Data Pipeline
--------------------
API de SpaceX -> JSON crudo -> transformación (pandas) -> controles de calidad
-> Postgres (warehouse) -> reporte por email. Alertas por email si algo falla.
"""
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task

from alerting import on_failure_alert, on_retry_alert
from email_report_operator import EmailReportOperator

default_args = {
    "owner": "javier",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_failure_alert,
    "on_retry_callback": on_retry_alert,
    "execution_timeout": timedelta(minutes=15),
}


@dag(
    dag_id="spacex_pipeline",
    description="ETL de lanzamientos y cohetes de SpaceX con controles de calidad y alertas",
    start_date=datetime(2026, 10, 1),
    schedule="0 9 * * *",  # todos los días 09:00 UTC
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["spacex", "etl", "portfolio"],
    doc_md=__doc__,
)
def spacex_pipeline():

    @task.sensor(poke_interval=30, timeout=60 * 10, mode="reschedule")
    def api_disponible() -> bool:
        from spacex_etl.extract import api_is_up
        return api_is_up()

    @task
    def extract_launches(ds=None) -> str:
        from spacex_etl.extract import extract
        return extract("launches", ds)

    @task
    def extract_rockets(ds=None) -> str:
        from spacex_etl.extract import extract
        return extract("rockets", ds)

    @task
    def transform(launches_path: str, rockets_path: str, ds=None) -> dict:
        from spacex_etl import config
        from spacex_etl.extract import read_json
        from spacex_etl.transform import transform_launches, transform_rockets

        rockets = transform_rockets(read_json(rockets_path))
        launches = transform_launches(read_json(launches_path), rockets)

        out = config.DATA_DIR / "processed" / ds
        out.mkdir(parents=True, exist_ok=True)
        launches.to_pickle(out / "launches.pkl")
        rockets.to_pickle(out / "rockets.pkl")
        return {"launches": str(out / "launches.pkl"), "rockets": str(out / "rockets.pkl")}

    @task
    def quality_checks(paths: dict) -> list:
        from spacex_etl.quality import run_checks
        checks = run_checks(pd.read_pickle(paths["launches"]), pd.read_pickle(paths["rockets"]))
        for c in checks:
            print(("✔" if c["passed"] else "✘"), c["check_name"], "-", c["detail"])
        return checks

    @task
    def load(paths: dict, checks: list, ds=None) -> dict:
        from spacex_etl.load import load_tables, save_quality_results
        from spacex_etl.quality import assert_checks

        # Guardamos los checks (para el dashboard) y si alguno falló, NO cargamos datos malos.
        save_quality_results(checks, ds)
        assert_checks(checks)

        launches = pd.read_pickle(paths["launches"])
        stats = {
            "exitos": int((launches["status"] == "success").sum()),
            "fallas": int((launches["status"] == "failure").sum()),
            "proximos": int((launches["status"] == "upcoming").sum()),
        }
        result = load_tables(launches, pd.read_pickle(paths["rockets"]))
        return {**result, **stats, "checks_ok": sum(c["passed"] for c in checks)}

    raw_l, raw_r = extract_launches(), extract_rockets()
    api_disponible() >> [raw_l, raw_r]
    paths = transform(raw_l, raw_r)
    checks = quality_checks(paths)
    loaded = load(paths, checks)

    reporte = EmailReportOperator(
        task_id="enviar_reporte",
        subject="[SpaceX Pipeline] Corrida del {{ ds }} OK",
        summary={
            "Fecha lógica": "{{ ds }}",
            "Lanzamientos cargados": "{{ ti.xcom_pull(task_ids='load')['launches'] }}",
            "Éxitos / Fallas / Próximos": (
                "{{ ti.xcom_pull(task_ids='load')['exitos'] }} / "
                "{{ ti.xcom_pull(task_ids='load')['fallas'] }} / "
                "{{ ti.xcom_pull(task_ids='load')['proximos'] }}"
            ),
            "Controles de calidad OK": "{{ ti.xcom_pull(task_ids='load')['checks_ok'] }}",
        },
    )
    loaded >> reporte


spacex_pipeline()
