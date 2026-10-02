"""Central configuration for paths used across the pipeline."""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
RAW_DB_PATH = DATA_DIR / "raw" / "source.db"

BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

LOGS_DIR = PROJECT_ROOT / "logs"
PIPELINE_LOG_PATH = LOGS_DIR / "pipeline_log.csv"
WATERMARK_STATE_PATH = LOGS_DIR / "cdc_watermarks.json"

for directory in (BRONZE_DIR, SILVER_DIR, GOLD_DIR, LOGS_DIR, RAW_DB_PATH.parent):
    directory.mkdir(parents=True, exist_ok=True)

# Tables tracked through the pipeline
SOURCE_TABLES = ["orders", "logistics"]
