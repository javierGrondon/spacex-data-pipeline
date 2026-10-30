from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, inspect, text

from . import config


def get_engine():
    return create_engine(config.WAREHOUSE_URL)


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def save_quality_results(checks: list[dict], run_date: str) -> int:
    """Guarda el resultado de los controles (idempotente por run_date)."""
    engine = get_engine()
    qc = pd.DataFrame(checks).assign(run_date=run_date, checked_at=_now())
    with engine.begin() as conn:
        if inspect(conn).has_table("quality_results"):
            conn.execute(text("DELETE FROM quality_results WHERE run_date = :d"), {"d": run_date})
        qc.to_sql("quality_results", conn, if_exists="append", index=False)
    return len(qc)


def load_tables(launches: pd.DataFrame, rockets: pd.DataFrame) -> dict:
    """Full refresh de las tablas (el dataset es chico, así es idempotente y simple)."""
    engine = get_engine()
    loaded_at = _now()
    with engine.begin() as conn:
        rockets.assign(loaded_at=loaded_at).to_sql("rockets", conn, if_exists="replace", index=False)
        launches.assign(loaded_at=loaded_at).to_sql("launches", conn, if_exists="replace", index=False)
    return {"launches": len(launches), "rockets": len(rockets)}
