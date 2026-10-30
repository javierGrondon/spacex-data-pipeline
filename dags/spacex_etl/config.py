import os
from pathlib import Path

API_BASE_URL = os.getenv("SPACEX_API_URL", "https://api.spacexdata.com/v4")
DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
WAREHOUSE_URL = os.getenv(
    "WAREHOUSE_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/warehouse"
)
# Si la API no responde, el pipeline puede usar los datos de muestra del repo.
USE_SAMPLE_DATA = os.getenv("USE_SAMPLE_DATA", "false").lower() == "true"
SAMPLE_DIR = Path(os.getenv("SAMPLE_DIR", Path(__file__).resolve().parents[2] / "sample_data"))
