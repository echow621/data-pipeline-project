"""
Orchestrates the full pipeline run: bronze -> silver -> gold -> data quality.
Every stage is logged to logs/pipeline_log.csv with a timestamp, row count
and status -- the same log-table pattern used to track daily production
data refreshes, just written to CSV here instead of a database table.

# Orchestration note: in production this script would typically be
# triggered by Airflow / Databricks Workflows on a schedule (e.g. hourly),
# rather than run manually.
"""
import csv
from datetime import datetime, timezone

from bronze_layer import run_bronze
from config import PIPELINE_LOG_PATH
from data_quality import run_data_quality
from gold_layer import run_gold
from silver_layer import run_silver


def _log(stage: str, table_or_check: str, row_count, status: str) -> None:
    is_new_file = not PIPELINE_LOG_PATH.exists()
    with open(PIPELINE_LOG_PATH, "a", newline="") as f:
        writer = csv.writer(f)
        if is_new_file:
            writer.writerow(["run_timestamp", "stage", "table", "row_count", "status"])
        writer.writerow(
            [datetime.now(timezone.utc).isoformat(), stage, table_or_check, row_count, status]
        )


def run_pipeline() -> None:
    print("Running BRONZE layer (incremental extract)...")
    bronze_results = run_bronze()
    for table, count in bronze_results.items():
        _log("bronze", table, count, "ok")
        print(f"  {table}: {count} new/changed rows")

    print("Running SILVER layer (clean + harmonize)...")
    silver_results = run_silver()
    for table, count in silver_results.items():
        _log("silver", table, count, "ok")
        print(f"  {table}: {count} rows")

    print("Running GOLD layer (aggregate KPIs)...")
    gold_results = run_gold()
    for table, count in gold_results.items():
        _log("gold", table, count, "ok")
        print(f"  {table}: {count} rows")

    print("Running data quality checks...")
    warnings = run_data_quality()
    if warnings:
        for w in warnings:
            _log("data_quality", w, "-", "warning")
            print(f"  WARNING: {w}")
    else:
        _log("data_quality", "all_checks", "-", "ok")
        print("  No issues found.")

    print(f"\nPipeline run complete. Log written to {PIPELINE_LOG_PATH}")


if __name__ == "__main__":
    run_pipeline()
