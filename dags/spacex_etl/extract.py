import json
import logging
import runpy
import shutil
import urllib.request
from pathlib import Path

from . import config

log = logging.getLogger(__name__)


def api_is_up(timeout: int = 10) -> bool:
    """Chequeo de salud de la API (lo usa el sensor)."""
    if config.USE_SAMPLE_DATA:
        return True
    try:
        with urllib.request.urlopen(f"{config.API_BASE_URL}/launches/latest", timeout=timeout) as r:
            return r.status == 200
    except Exception as exc:  # noqa: BLE001
        log.warning("API de SpaceX no disponible todavía: %s", exc)
        return False


def ensure_sample_data() -> None:
    """Genera los datos de muestra si todavía no existen (no se versionan en git)."""
    if not (config.SAMPLE_DIR / "launches.json").exists():
        runpy.run_path(str(config.SAMPLE_DIR / "generate_sample.py"), run_name="__main__")


def extract(endpoint: str, run_date: str) -> str:
    """Descarga /<endpoint> y guarda el JSON crudo en DATA_DIR/raw/<run_date>/."""
    out_dir = config.DATA_DIR / "raw" / run_date
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{endpoint}.json"

    if config.USE_SAMPLE_DATA:
        ensure_sample_data()
        shutil.copy(config.SAMPLE_DIR / f"{endpoint}.json", out_path)
        log.info("Modo muestra: copiado %s", out_path)
    else:
        url = f"{config.API_BASE_URL}/{endpoint}"
        req = urllib.request.Request(url, headers={"User-Agent": "spacex-pipeline/1.0"})
        with urllib.request.urlopen(req, timeout=60) as r:
            payload = json.loads(r.read())
        out_path.write_text(json.dumps(payload))
        log.info("Descargados %s registros de %s", len(payload), url)

    return str(out_path)


def read_json(path: str) -> list:
    return json.loads(Path(path).read_text())
