"""Controles de calidad de datos. Cada check devuelve (nombre, pasó, detalle)."""
import pandas as pd


class DataQualityError(Exception):
    pass


def run_checks(launches: pd.DataFrame, rockets: pd.DataFrame) -> list[dict]:
    checks = []

    def add(name, passed, detail=""):
        checks.append({"check_name": name, "passed": bool(passed), "detail": detail})

    add("launches_no_vacio", len(launches) > 0, f"{len(launches)} filas")
    add("rockets_no_vacio", len(rockets) > 0, f"{len(rockets)} filas")

    dup = launches["launch_id"].duplicated().sum()
    add("launch_id_unico", dup == 0, f"{dup} duplicados")

    null_dates = launches["date_utc"].isna().sum()
    add("fecha_sin_nulos", null_dates == 0, f"{null_dates} fechas nulas")

    orphan = launches["rocket_name"].isna().sum()
    add("cohete_existe", orphan == 0, f"{orphan} lanzamientos sin cohete conocido")

    bad_status = (~launches["status"].isin(["success", "failure", "upcoming", "unknown"])).sum()
    add("status_valido", bad_status == 0, f"{bad_status} estados inválidos")

    # Un lanzamiento futuro no debería tener resultado
    weird = (launches["upcoming"] & launches["success"].notna()).sum()
    add("upcoming_sin_resultado", weird == 0, f"{weird} inconsistentes")

    return checks


def assert_checks(checks: list[dict]) -> None:
    failed = [c for c in checks if not c["passed"]]
    if failed:
        detail = "; ".join(f"{c['check_name']} ({c['detail']})" for c in failed)
        raise DataQualityError(f"Fallaron {len(failed)} controles de calidad: {detail}")
