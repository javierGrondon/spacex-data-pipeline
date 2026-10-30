import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "dags"), str(ROOT / "plugins")]

os.environ.setdefault("USE_SAMPLE_DATA", "true")
os.environ.setdefault("SAMPLE_DIR", str(ROOT / "sample_data"))

# Los datos de muestra no se versionan: se generan la primera vez
if not (ROOT / "sample_data" / "launches.json").exists():
    import runpy
    runpy.run_path(str(ROOT / "sample_data" / "generate_sample.py"), run_name="__main__")
