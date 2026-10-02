# Logistics & Orders Data Pipeline (Bronze / Silver / Gold)

A small end-to-end data engineering project simulating source-system integration,
incremental loading (Change Data Capture), data quality checks, and KPI aggregation.
Built to demonstrate the core skills required for a Senior Data Engineer role
(ETL/ELT, data modeling, CDC, monitoring, data quality, orchestration).

## Architecture

```
Source system (SQLite: orders, logistics)
        |
        v
   BRONZE layer   -- raw extract, incremental via CDC watermark (updated_at)
        |
        v
   SILVER layer   -- cleaned, typed, deduplicated, harmonized
        |
        v
   GOLD layer     -- aggregated KPIs (daily order volume, SLA metrics)
```

Each layer is a separate script under `src/`, orchestrated by `src/pipeline.py`.
Every run is logged to `logs/pipeline_log.csv` (table name, run timestamp, row count,
status) -- the same pattern used for production monitoring of daily data refreshes.

## Why this project

| Job requirement                          | Covered by                                   |
|-------------------------------------------|-----------------------------------------------|
| Batch processing                          | `bronze_layer.py`, `silver_layer.py`          |
| Change Data Capture                       | `cdc_loader.py` (watermark-based incremental) |
| Data modeling (Bronze/Silver/Gold)        | Folder structure + `gold_layer.py`            |
| Data quality / validation                 | `data_quality.py`                             |
| Monitoring / Data Observability           | `logs/pipeline_log.csv`                       |
| Automation / orchestration                | `pipeline.py`                                 |
| Git / CI/CD / automated tests             | `tests/`, `.github/workflows/ci.yml`          |
| SQL & Python                              | SQLite source + pandas transformations        |

The design maps directly onto a Databricks Bronze/Silver/Gold setup -- swapping
pandas for PySpark DataFrames and SQLite for a Delta table source would port this
project to Databricks with minimal changes (noted as comments in the code).

## Setup

```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Run

```bash
# 1. Generate a simulated source database (run once)
python src/generate_source_data.py

# 2. Run the full pipeline (bronze -> silver -> gold + quality checks)
python src/pipeline.py

# 3. Simulate new/changed source records and re-run to see CDC in action
python src/generate_source_data.py --simulate-changes
python src/pipeline.py
```

## Tests

```bash
pytest tests/
```

## Next steps / extension ideas

- Swap SQLite source for a real open dataset (e.g. a logistics dataset from Kaggle)
- Port `silver_layer.py` / `gold_layer.py` to PySpark and run on Databricks Community Edition
- Add a simple dashboard (Streamlit) on top of the Gold layer
- Add a streaming variant using a message queue (e.g. Kafka via `confluent-kafka` or a
  local `queue`-based simulation) to also demonstrate streaming, not just batch+CDC

## Databricks version

See `databricks/` for a PySpark/Delta port of this project (same Bronze/Silver/Gold
design, `MERGE INTO` for CDC upserts, Databricks Asset Bundle for automated deployment,
and a CI/CD step in `.github/workflows/ci.yml` that deploys on push to `main`).
See `databricks/README.md` for setup and important notes on workspace-tier limitations.
