"""
Simple watermark-based Change Data Capture (CDC).

Real systems often use log-based CDC (e.g. reading a database's transaction
log, or a tool like Debezium). For this project a lighter, widely-used
pattern is used instead: track the maximum `updated_at` value loaded so far
per table, and on the next run only pull rows newer than that watermark.
This is a common approach for source systems that expose an `updated_at`
column but no changelog (e.g. many REST APIs and legacy databases).
"""
import json

from config import WATERMARK_STATE_PATH


def load_watermarks() -> dict:
    if WATERMARK_STATE_PATH.exists():
        return json.loads(WATERMARK_STATE_PATH.read_text())
    return {}


def save_watermark(table_name: str, watermark_value: str) -> None:
    watermarks = load_watermarks()
    watermarks[table_name] = watermark_value
    WATERMARK_STATE_PATH.write_text(json.dumps(watermarks, indent=2))


def get_watermark(table_name: str) -> str | None:
    return load_watermarks().get(table_name)
